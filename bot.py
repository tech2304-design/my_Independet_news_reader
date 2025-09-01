from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from typing import Callable

HELP_TEXT = (
    "Команды:\n"
    "/latest [N] — последние N новостей (по умолчанию 10)\n"
    "/news — то же, что /latest\n"
    "/stats — статистика по источникам\n"
    "/help — помощь\n"
)

def format_headline(item: dict, idx: int) -> str:
    base = f"{idx}. [{item['source']}] {item['title']}"
    if item.get("published"):
        base += f"\n{item['published']}"
    return base

def build_application(token: str):
    return Application.builder().token(token).build()

def register_handlers(app: Application, db, fetch_trigger: Callable):
    async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(HELP_TEXT)

    async def latest_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        args = context.args
        try:
            limit = int(args[0]) if args else 10
            limit = max(1, min(limit, 30))
        except ValueError:
            limit = 10
        rows = db.latest(limit=limit)
        if not rows:
            await update.message.reply_text("Нет новостей в базе.")
            return
        # Отправляем каждую новость отдельным сообщением с кнопкой
        for idx, r in enumerate(rows, 1):
            text = format_headline(r, idx)
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton(text="Открыть", url=r["link"])]]
            )
            await update.message.reply_text(text, reply_markup=kb, disable_web_page_preview=True)

    async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
        stats = db.count_by_source()
        total = db.total()
        lines = [f"Всего новостей: {total}"]
        for s in stats[:30]:
            lines.append(f"{s['source']}: {s['count']}")
        await update.message.reply_text("\n".join(lines))

    async def news_alias(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await latest_cmd(update, context)

    async def force_fetch(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Запускаю принудительный сбор...")
        added_map = await fetch_trigger()
        if sum(added_map.values()) == 0:
            await update.message.reply_text("Новых новостей не найдено.")
        else:
            lines = ["Принудительный сбор завершён:"]
            for k, v in added_map.items():
                if v:
                    lines.append(f"- {k}: {v}")
            await update.message.reply_text("\n".join(lines))

    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("latest", latest_cmd))
    app.add_handler(CommandHandler("news", news_alias))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("fetch", force_fetch))  # опционально /fetch админ
