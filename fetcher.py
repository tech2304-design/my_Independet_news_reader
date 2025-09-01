import hashlib
import feedparser
import aiohttp
from datetime import datetime
from typing import Dict, List, Any, Optional

def _hash_item(link: str, title: str) -> str:
    h = hashlib.sha1()
    h.update((link + "|" + title).encode("utf-8"))
    return h.hexdigest()

def normalize_date(entry) -> Optional[str]:
    dt_struct = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if dt_struct:
        try:
            return datetime(*dt_struct[:6]).isoformat()
        except Exception:
            return None
    return None

async def fetch_single(session: aiohttp.ClientSession, url: str, timeout: int):
    try:
        async with session.get(url, timeout=timeout) as resp:
            data = await resp.read()
        return feedparser.parse(data)
    except Exception:
        return feedparser.parse(b"")

async def collect_all(
    feeds: Dict[str, str],
    user_agent: str,
    timeout: int = 20,
    batch_limit_per_feed: int = 0
) -> List[Dict[str, Any]]:
    headers = {"User-Agent": user_agent}
    items: List[Dict[str, Any]] = []
    async with aiohttp.ClientSession(headers=headers) as session:
        for source, url in feeds.items():
            parsed = await fetch_single(session, url, timeout=timeout)
            entries = parsed.entries or []
            if batch_limit_per_feed > 0:
                entries = entries[:batch_limit_per_feed]
            for e in entries:
                link = getattr(e, "link", "") or e.get("link")
                title = getattr(e, "title", "") or e.get("title")
                if not link or not title:
                    continue
                summary = getattr(e, "summary", "") or e.get("summary", "")
                items.append({
                    "source": source,
                    "title": title.strip(),
                    "link": link.strip(),
                    "summary": summary.strip(),
                    "published": normalize_date(e),
                    "hash": _hash_item(link, title),
                })
    return items