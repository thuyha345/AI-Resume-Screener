import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple


class Database:
    def __init__(self, db_path: str = "storage/app.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path.as_posix())
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS resumes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    language TEXT DEFAULT 'unknown',
                    file_path TEXT,
                    raw_text TEXT,
                    ocr_used INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL
                );
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS extractions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    resume_id INTEGER NOT NULL,
                    extracted_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (resume_id) REFERENCES resumes(id) ON DELETE CASCADE
                );
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS screening_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    resume_id INTEGER NOT NULL,
                    job_title TEXT,
                    score REAL,
                    reasoning TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (resume_id) REFERENCES resumes(id) ON DELETE CASCADE
                );
            """)

    @staticmethod
    def _now_iso() -> str:
        return datetime.utcnow().isoformat(timespec="seconds")

    def insert_resume(
        self,
        filename: str,
        language: str = "unknown",
        file_path: Optional[str] = None,
        raw_text: Optional[str] = None,
        ocr_used: bool = False
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO resumes (filename, language, file_path, raw_text, ocr_used, created_at)
                   VALUES (?, ?, ?, ?, ?, ?);""",
                (filename, language, file_path, raw_text, int(ocr_used), self._now_iso())
            )
            return int(cur.lastrowid)

    def insert_extraction(self, resume_id: int, extracted_data: Dict[str, Any]) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO extractions (resume_id, extracted_json, created_at)
                   VALUES (?, ?, ?);""",
                (resume_id, json.dumps(extracted_data, ensure_ascii=False), self._now_iso())
            )
            return int(cur.lastrowid)

    def insert_screening_result(self, resume_id: int, job_title: str, score: float, reasoning: str = "") -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO screening_results (resume_id, job_title, score, reasoning, created_at)
                   VALUES (?, ?, ?, ?, ?);""",
                (resume_id, job_title, score, reasoning, self._now_iso())
            )
            return int(cur.lastrowid)

    def list_resumes(self, limit: int = 50) -> List[Tuple]:
        with self._connect() as conn:
            cur = conn.execute(
                """SELECT id, filename, language, ocr_used, created_at
                   FROM resumes ORDER BY id DESC LIMIT ?;""",
                (limit,)
            )
            return cur.fetchall()
