import sqlite3
from typing import List, Dict, Any
from contextlib import contextmanager
from search_parser import parse_user_query, build_fts_query, build_like_sql

SCHEMA = """
CREATE TABLE IF NOT EXISTS news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    link TEXT NOT NULL,
    published TEXT,
    summary TEXT,
    hash TEXT NOT NULL UNIQUE,
    added_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_news_published ON news(published);
CREATE INDEX IF NOT EXISTS idx_news_source ON news(source);

CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(
    title,
    summary,
    content='news',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS news_ai AFTER INSERT ON news BEGIN
  INSERT INTO news_fts(rowid, title, summary) VALUES (new.id, new.title, new.summary);
END;
CREATE TRIGGER IF NOT EXISTS news_ad AFTER DELETE ON news BEGIN
  INSERT INTO news_fts(news_fts, rowid, title, summary) VALUES ('delete', old.id, old.title, old.summary);
END;
CREATE TRIGGER IF NOT EXISTS news_au AFTER UPDATE ON news BEGIN
  INSERT INTO news_fts(news_fts, rowid, title, summary) VALUES ('delete', old.id, old.title, old.summary);
  INSERT INTO news_fts(rowid, title, summary) VALUES (new.id, new.title, new.summary);
END;
"""

class Database:
    def __init__(self, path: str):
        self.path = path
        self._fts_available = True
        self._init()

    def _init(self):
        with self.connection() as conn:
            try:
                conn.executescript(SCHEMA)
                try:
                    conn.execute("SELECT count(*) FROM news_fts")
                except sqlite3.OperationalError:
                    self._fts_available = False
            except sqlite3.OperationalError as e:
                if "fts5" in str(e).lower():
                    self._fts_available = False
                    base_schema_lines = []
                    for line in SCHEMA.splitlines():
                        if ("VIRTUAL TABLE" in line) or ("news_ai" in line) or ("news_ad" in line) or ("news_au" in line):
                            continue
                        base_schema_lines.append(line)
                    conn.executescript("\n".join(base_schema_lines))
                else:
                    raise

    @contextmanager
    def connection(self):
        conn = sqlite3.connect(self.path)
        try:
            yield conn
        finally:
            conn.close()

    def insert_many(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        inserted = 0
        with self.connection() as conn:
            cur = conn.cursor()
            for r in rows:
                try:
                    cur.execute(
                        """INSERT INTO news(source, title, link, published, summary, hash)
                           VALUES(?,?,?,?,?,?)""",
                        (
                            r["source"],
                            r["title"],
                            r["link"],
                            r.get("published"),
                            r.get("summary"),
                            r["hash"],
                        ),
                    )
                    inserted += 1
                except sqlite3.IntegrityError:
                    pass
            conn.commit()
        return inserted

    def latest(self, limit: int = 10):
        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT source, title, link, published, summary, added_at
                   FROM news
                   ORDER BY datetime(published) DESC, id DESC
                   LIMIT ?""",
                (limit,),
            )
            rows = cur.fetchall()
        return [
            {
                "source": r[0],
                "title": r[1],
                "link": r[2],
                "published": r[3],
                "summary": r[4],
                "added_at": r[5],
            }
            for r in rows
        ]

    def latest_page(self, offset: int, limit: int):
        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT source, title, link, published, summary, added_at
                   FROM news
                   ORDER BY datetime(published) DESC, id DESC
                   LIMIT ? OFFSET ?""",
                (limit, offset),
            )
            rows = cur.fetchall()
        return [
            {
                "source": r[0],
                "title": r[1],
                "link": r[2],
                "published": r[3],
                "summary": r[4],
                "added_at": r[5],
            }
            for r in rows
        ]

    def count_by_source(self):
        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT source, COUNT(*) FROM news GROUP BY source ORDER BY COUNT(*) DESC"
            )
            rows = cur.fetchall()
        return [{"source": r[0], "count": r[1]} for r in rows]

    def total(self) -> int:
        with self.connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM news")
            return cur.fetchone()[0]

    def search(self, query: str, limit: int, offset: int):
        raw = (query or "").strip()
        if not raw:
            return [], 0
        ast = parse_user_query(raw)
        if not ast:
            return [], 0
        with self.connection() as conn:
            cur = conn.cursor()
            if self._fts_available:
                fts_q = build_fts_query(ast)
                cur.execute(
                    "SELECT COUNT(*) FROM news_fts WHERE news_fts MATCH ?",
                    (fts_q,),
                )
                total = cur.fetchone()[0]
                cur.execute(
                    """SELECT n.source, n.title, n.link, n.published, n.summary, n.added_at
                       FROM news_fts
                       JOIN news n ON n.id = news_fts.rowid
                       WHERE news_fts MATCH ?
                       ORDER BY n.id DESC
                       LIMIT ? OFFSET ?""",
                    (fts_q, limit, offset),
                )
            else:
                where_sql, params = build_like_sql(ast, title_col="title", summary_col="summary")
                cur.execute(
                    f"SELECT COUNT(*) FROM news WHERE {where_sql}",
                    params,
                )
                total = cur.fetchone()[0]
                cur.execute(
                    f"""SELECT source, title, link, published, summary, added_at
                        FROM news
                        WHERE {where_sql}
                        ORDER BY id DESC
                        LIMIT ? OFFSET ?""",
                    params + [limit, offset],
                )
            rows = cur.fetchall()
        return ([
            {
                "source": r[0],
                "title": r[1],
                "link": r[2],
                "published": r[3],
                "summary": r[4],
                "added_at": r[5],
            }
            for r in rows
        ], total)