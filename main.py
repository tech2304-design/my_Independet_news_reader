import os
import asyncio
import toml
from typing import Dict
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from feeds import FEEDS
from db import Database
from fetcher import collect_all
from bot_commands import router, setup_handlers
from scheduler import setup_scheduler

CONFIG_PATH = "config.toml"

def load_config():
    data = {}
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = toml.load(f)

    cfg = {
        "db_path": os.getenv("DB_PATH") or data.get("database", {}).get("path", "news.db"),
        "bot_token": os.getenv("BOT_TOKEN") or data.get("telegram", {}).get("bot_token"),
        "chat_id": os.getenv("CHAT_ID") or data.get("telegram", {}).get("chat_id"),
        "interval_minutes": data.get("scheduler", {}).get("fetch_interval_minutes", 60),
        "batch_limit": data.get("fetch", {}).get("batch_limit_per_feed", 0),
        "timeout_seconds": data.get("fetch", {}).get("timeout_seconds", 20),
        "user_agent": data.get("fetch", {}).get("user_agent", "RSSBotAiogram/1.0 (+https://example.org)"),
        "page_size": data.get("pagination", {}).get("page_size", 10),
        "search_page_size": data.get("pagination", {}).get("search_page_size", None),
        "latest_count": data.get("pagination", {}).get("latest_count", None),
    }
    missing = [k for k in ("bot_token", "chat_id") if not cfg[k]]
    if missing:
        raise RuntimeError(f"Отсутствуют обязательные настройки: {missing}")
    cfg["chat_id"] = int(cfg["chat_id"])
    return cfg

async def build_runtime():
    cfg = load_config()
    db = Database(cfg["db_path"])
    bot = Bot(
        token=cfg["bot_token"],
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML,
        )
    )
    dp = Dispatcher()

    async def do_fetch() -> Dict[str, int]:
        all_items = await collect_all(
            FEEDS,
            user_agent=cfg["user_agent"],
            timeout=cfg["timeout_seconds"],
            batch_limit_per_feed=cfg["batch_limit"]
        )
        added_per_source: Dict[str, int] = {s: 0 for s in FEEDS.keys()}
        grouped = {s: [] for s in FEEDS.keys()}
        for it in all_items:
            grouped[it["source"]].append(it)
        for source, rows in grouped.items():
            inserted = db.insert_many(rows)
            added_per_source[source] = inserted
        return added_per_source

    async def scheduled_fetch():
        added_map = await do_fetch()
        total_new = sum(added_map.values())
        if total_new == 0:
            return added_map
        lines = [f"Новые новости: {total_new}"]
        for s, cnt in added_map.items():
            if cnt:
                lines.append(f"- {s}: {cnt}")
        await bot.send_message(chat_id=cfg["chat_id"], text="\n".join(lines))
        return added_map

    async def fetch_trigger():
        return await scheduled_fetch()

    setup_handlers(
        db,
        fetch_trigger,
        chat_id_admin=cfg["chat_id"],
        page_size=cfg["page_size"],
        search_page_size=cfg["search_page_size"],
        latest_count=cfg["latest_count"] or cfg["page_size"],
    )
    dp.include_router(router)

    return cfg, bot, dp, scheduled_fetch

async def main():
    cfg, bot, dp, scheduled_fetch = await build_runtime()
    await scheduled_fetch()
    setup_scheduler(lambda: asyncio.create_task(scheduled_fetch()), cfg["interval_minutes"])
    print("Bot started.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "event loop is already running" in str(e):
            print("Обнаружен активный цикл. Fallback режим.")
            import nest_asyncio
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            loop.create_task(main())
        else:
            raise