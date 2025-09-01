from aiogram import Router
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData
from typing import Callable, Dict
import hashlib
import html
import re

router = Router()

HELP_TEXT = (
    "Бот агрегирует RSS независимых медиа, хранит новости в SQLite и даёт поиск (FTS / логические операторы).\n\n"
    "Команды:\n"
    "/start — краткая справка\n"
    "/help — эта справка\n"
    "/latest — последние фиксированное число новостей (списком)\n"
    "/news [N] — показать одну новость (по умолчанию первую). N — номер (1-based)\n"
    "/filter &lt;запрос&gt; — поиск (AND OR NOT, + - |, \"фразы\"; пробел = AND)\n"
    "/stats — статистика по источникам\n"
    "/fetch — принудительный сбор (админ)\n\n"
    "Навигация:\n"
    "- В списках: «Пред» / «След» / «Закрыть»\n"
    "- В одиночном просмотре (/news): «⏮ Перв.» «« Пред» «След » «Посл. ⏭» + «✖ Закрыть».\n"
    "Скобки ( ) в поиске не поддерживаются (ограничение FTS5)."
)

# ----- CallbackData -----

class LatestPage(CallbackData, prefix="lp"):
    offset: int
    limit: int

class FilterPage(CallbackData, prefix="fs"):
    key: str
    offset: int
    limit: int

class NewsItem(CallbackData, prefix="ni"):
    idx: int

# ----- Текстовая очистка / экранирование -----

TAG_RE = re.compile(r"<[^>]+>")
BRACKET_ENTITY_RE = re.compile(r"\[&#\d+;?\]")        # пример: [&#8230;]
MULTISPACE_RE = re.compile(r"[ \t\r\f\v]+")
NEWLINE_RE = re.compile(r"\n{3,}")
NBSP_RE = re.compile(r"\u00A0")

def clean_text(raw: str) -> str:
    """
    Приводит произвольный HTML-фрагмент (заголовок / summary) к безопасному тексту:
      1. html.unescape — декодируем сущности (&amp; -> &)
      2. Удаляем теги полностью.
      3. Заменяем [&#8230;] и подобные шаблоны на многоточие.
      4. Приводим множественные пробелы к одному, убираем неразрывные пробелы.
      5. Подрезаем края.
      6. Экранируем для HTML (parse_mode=HTML).
    """
    if not raw:
        return ""
    # Декод сущностей
    txt = html.unescape(raw)
    # Удаляем теги
    txt = TAG_RE.sub("", txt)
    # Удаляем JS/CSS артефакты (примитивно)
    # Можно добавить дополнительные фильтры при необходимости
    # Заменяем [&#8230;] на …
    txt = BRACKET_ENTITY_RE.sub("…", txt)
    # Неразрывные пробелы -> обычные
    txt = NBSP_RE.sub(" ", txt)
    # Много пробелов -> один
    txt = MULTISPACE_RE.sub(" ", txt)
    # Слишком много подряд переводов строк -> максимум два
    txt = NEWLINE_RE.sub("\n\n", txt)
    txt = txt.strip()
    # Финальное экранирование
    return html.escape(txt)

def safe_join(parts):
    return "\n\n".join(p for p in parts if p)

# ----- Формирование списков -----

def format_item_line(item: dict, idx: int) -> str:
    title = clean_text(item.get('title') or "")
    source = clean_text(item.get('source') or "")
    published = clean_text(item.get('published') or "")
    line = f"{idx}. [{source}] {title}"
    if published:
        line += f"\n{published}"
    return line

def build_page_text(items: list, offset: int, limit: int, total: int, header: str) -> str:
    header = clean_text(header)
    if not items:
        if total == 0:
            return f"{header}\nНет данных."
        return f"{header}\nЭта страница пуста."
    lines = [f"{header}\nПозиции {offset+1}–{offset+len(items)} из {total}"]
    for i, it in enumerate(items, start=1):
        lines.append(format_item_line(it, offset + i))
    return "\n\n".join(lines)

def build_news_keyboard(items: list, offset: int, limit: int, total: int):
    buttons = []
    for i, it in enumerate(items, start=1):
        buttons.append([InlineKeyboardButton(text=f"🔗 {offset + i}", url=it["link"])])
    has_prev = offset > 0
    has_next = (offset + limit) < total
    nav_row = []
    if has_prev:
        nav_row.append(
            InlineKeyboardButton(
                text="« Пред",
                callback_data=LatestPage(offset=max(0, offset - limit), limit=limit).pack(),
            )
        )
    if has_next:
        nav_row.append(
            InlineKeyboardButton(
                text="След »",
                callback_data=LatestPage(offset=offset + limit, limit=limit).pack(),
            )
        )
    if nav_row:
        buttons.append(nav_row)
    buttons.append([InlineKeyboardButton(text="✖ Закрыть", callback_data="lp:close")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def build_search_keyboard(items: list, key: str, offset: int, limit: int, total: int):
    buttons = []
    for i, it in enumerate(items, start=1):
        buttons.append([InlineKeyboardButton(text=f"🔗 {offset + i}", url=it["link"])])
    has_prev = offset > 0
    has_next = (offset + limit) < total
    nav_row = []
    if has_prev:
        nav_row.append(
            InlineKeyboardButton(
                text="« Пред",
                callback_data=FilterPage(key=key, offset=max(0, offset - limit), limit=limit).pack(),
            )
        )
    if has_next:
        nav_row.append(
            InlineKeyboardButton(
                text="След »",
                callback_data=FilterPage(key=key, offset=offset + limit, limit=limit).pack(),
            )
        )
    if nav_row:
        buttons.append(nav_row)
    buttons.append([InlineKeyboardButton(text="✖ Закрыть", callback_data="fs:close")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ----- Одна новость -----

def build_single_news_text(item: dict, idx: int, total: int) -> str:
    title = clean_text(item.get("title") or "")
    source = clean_text(item.get("source") or "")
    published = clean_text(item.get("published") or "")
    summary = clean_text(item.get("summary") or "")
    parts = [
        f"Новость {idx+1} из {total}",
        f"[{source}] {title}",
        published,
        summary
    ]
    return safe_join(parts)

def build_single_news_keyboard(item: dict, idx: int, total: int):
    buttons = []
    buttons.append([InlineKeyboardButton(text="🔗 Перейти", url=item["link"])])

    nav_rows = []

    left_row = []
    if idx > 0:
        left_row.append(
            InlineKeyboardButton(
                text="⏮ Перв.",
                callback_data=NewsItem(idx=0).pack()
            )
        )
        left_row.append(
            InlineKeyboardButton(
                text="« Пред",
                callback_data=NewsItem(idx=idx - 1).pack()
            )
        )
    if left_row:
        nav_rows.append(left_row)

    right_row = []
    if idx < total - 1:
        right_row.append(
            InlineKeyboardButton(
                text="След »",
                callback_data=NewsItem(idx=idx + 1).pack()
            )
        )
        right_row.append(
            InlineKeyboardButton(
                text="Посл. ⏭",
                callback_data=NewsItem(idx=total - 1).pack()
            )
        )
    if right_row:
        nav_rows.append(right_row)

    buttons.extend(nav_rows)

    buttons.append([InlineKeyboardButton(text="✖ Закрыть", callback_data="ni:close")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ----- Handlers -----

def setup_handlers(
    db,
    fetch_trigger: Callable,
    chat_id_admin: int | None = None,
    page_size: int = 10,
    search_page_size: int | None = None,
    latest_count: int | None = None,
):
    if search_page_size is None:
        search_page_size = page_size
    if latest_count is None:
        latest_count = page_size

    SEARCH_CACHE: Dict[str, str] = {}

    @router.message(Command("help"))
    @router.message(Command("start"))
    async def help_cmd(message: Message):
        await message.answer(HELP_TEXT)

    @router.message(Command("latest"))
    async def latest_cmd(message: Message):
        limit = max(1, latest_count)
        items = db.latest(limit)
        total = db.total()
        text = build_page_text(items, 0, limit, total, header=f"Последние {limit} новостей")
        kb = build_news_keyboard(items, 0, limit, total)
        await message.answer(text, reply_markup=kb, disable_web_page_preview=True)

    @router.message(Command("news"))
    async def news_cmd(message: Message):
        """
        /news [N]
        N — 1-based позиция новости (свежие сначала). Без аргумента = 1.
        """
        total = db.total()
        if total == 0:
            await message.answer("Нет новостей.")
            return
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            try:
                idx_user = int(parts[1])
                idx = idx_user - 1
            except (ValueError, TypeError):
                idx = 0
        else:
            idx = 0
        if idx < 0:
            idx = 0
        if idx >= total:
            idx = total - 1
        items = db.latest_page(idx, 1)
        if not items:
            await message.answer("Нет данных.")
            return
        item = items[0]
        text = build_single_news_text(item, idx, total)
        kb = build_single_news_keyboard(item, idx, total)
        await message.answer(text, reply_markup=kb, disable_web_page_preview=True)

    @router.message(Command("stats"))
    async def stats_cmd(message: Message):
        stats = db.count_by_source()
        total = db.total()
        lines = [f"Всего новостей: {total}"]
        for s in stats:
            lines.append(f"{clean_text(s['source'])}: {s['count']}")
        await message.answer("\n".join(lines))

    @router.message(Command("fetch"))
    async def fetch_cmd(message: Message):
        if chat_id_admin and message.from_user and message.from_user.id != chat_id_admin:
            await message.answer("Недостаточно прав.")
            return
        await message.answer("Запускаю сбор...")
        added_map = await fetch_trigger()
        total_new = sum(added_map.values())
        if total_new == 0:
            await message.answer("Новых новостей не найдено.")
        else:
            lines = [f"Новые новости: {total_new}"]
            for k, v in added_map.items():
                if v:
                    lines.append(f"- {clean_text(k)}: {v}")
            await message.answer("\n".join(lines))

    @router.message(Command("filter"))
    async def filter_cmd(message: Message):
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip():
            await message.answer(
                "Использование: /filter &lt;запрос&gt;\nПример: /filter коррупция OR взятка -\"старое дело\""
            )
            return
        raw_query = parts[1].strip()
        norm = raw_query.lower()
        key = hashlib.sha1(norm.encode("utf-8")).hexdigest()[:8]
        SEARCH_CACHE[key] = norm

        offset = 0
        limit = search_page_size
        rows, total = db.search(norm, limit, offset)
        header = f"Поиск: “{clean_text(raw_query)}”"
        text = build_page_text(rows, offset, limit, total, header=header)
        kb = build_search_keyboard(rows, key, offset, limit, total)
        await message.answer(text, reply_markup=kb, disable_web_page_preview=True)

    @router.callback_query()
    async def pagination_callback(cb: CallbackQuery):
        if not cb.data:
            return

        # Закрытия
        if cb.data in {"lp:close", "fs:close", "ni:close"}:
            try:
                await cb.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await cb.answer("Закрыто")
            return

        # Списочная пагинация (/latest)
        if cb.data.startswith("lp:"):
            try:
                data = LatestPage.unpack(cb.data)
            except Exception:
                await cb.answer("Ошибка данных", show_alert=False)
                return
            offset = data.offset
            limit = data.limit
            items = db.latest_page(offset, limit)
            total = db.total()
            text = build_page_text(items, offset, limit, total, header="Новости")
            kb = build_news_keyboard(items, offset, limit, total)
            try:
                await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
            except Exception:
                await cb.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
            await cb.answer()
            return

        # Поиск
        if cb.data.startswith("fs:"):
            try:
                data = FilterPage.unpack(cb.data)
            except Exception:
                await cb.answer("Ошибка данных", show_alert=False)
                return
            key = data.key
            offset = data.offset
            limit = data.limit
            if key not in SEARCH_CACHE:
                await cb.answer("Сессия поиска устарела. Повторите /filter.", show_alert=True)
                return
            norm = SEARCH_CACHE[key]
            rows, total = db.search(norm, limit, offset)
            header = f"Поиск: “{clean_text(norm)}”"
            text = build_page_text(rows, offset, limit, total, header=header)
            kb = build_search_keyboard(rows, key, offset, limit, total)
            try:
                await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
            except Exception:
                await cb.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
            await cb.answer()
            return

        # Одна новость (/news)
        if cb.data.startswith("ni:"):
            try:
                data = NewsItem.unpack(cb.data)
            except Exception:
                await cb.answer("Ошибка данных", show_alert=False)
                return
            idx = data.idx
            total = db.total()
            if total == 0:
                await cb.answer("Нет данных.", show_alert=False)
                return
            if idx < 0:
                idx = 0
            if idx >= total:
                idx = total - 1
            items = db.latest_page(idx, 1)
            if not items:
                await cb.answer("Нет данных.", show_alert=False)
                return
            item = items[0]
            text = build_single_news_text(item, idx, total)
            kb = build_single_news_keyboard(item, idx, total)
            try:
                await cb.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)
            except Exception:
                await cb.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
            await cb.answer()
            return