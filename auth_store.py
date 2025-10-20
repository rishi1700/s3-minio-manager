import sqlite3
import base64
import hashlib
import secrets
import time
from pathlib import Path
from typing import Optional, Tuple

CONFIG_DIR = Path.home() / ".s3_minio_manager"
DB_PATH = CONFIG_DIR / "auth.db"

PBKDF2_ITERATIONS = 240_000
SALT_BYTES = 16


def _ensure_db() -> sqlite3.Connection:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            iterations INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_login TEXT
        )
        """
    )
    conn.commit()
    return conn


def _hash_password(password: str, salt: Optional[bytes] = None, iterations: int = PBKDF2_ITERATIONS) -> Tuple[str, str, int]:
    if salt is None:
        salt = secrets.token_bytes(SALT_BYTES)
    if isinstance(salt, str):
        salt = base64.b64decode(salt)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return base64.b64encode(dk).decode("ascii"), base64.b64encode(salt).decode("ascii"), iterations


def user_count() -> int:
    with _ensure_db() as conn:
        cur = conn.execute("SELECT COUNT(*) FROM users")
        value, = cur.fetchone()
    return int(value or 0)


def get_user(username: str):
    with _ensure_db() as conn:
        cur = conn.execute(
            "SELECT id, username, password_hash, salt, iterations FROM users WHERE username = ?",
            (username.strip().lower(),),
        )
        row = cur.fetchone()
    return row


def create_user(username: str, password: str):
    username_norm = username.strip().lower()
    password_hash, salt, iterations = _hash_password(password)
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    with _ensure_db() as conn:
        conn.execute(
            """
            INSERT INTO users (username, password_hash, salt, iterations, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (username_norm, password_hash, salt, iterations, now, now),
        )
        conn.commit()


def verify_user(username: str, password: str) -> Optional[int]:
    username_norm = username.strip().lower()
    with _ensure_db() as conn:
        cur = conn.execute(
            "SELECT id, password_hash, salt, iterations FROM users WHERE username = ?",
            (username_norm,),
        )
        row = cur.fetchone()
        if not row:
            return None
        user_id, stored_hash, salt, iterations = row
        computed, _, _ = _hash_password(password, salt=salt, iterations=iterations)
        if not secrets.compare_digest(computed, stored_hash):
            return None
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE users SET last_login = ?, updated_at = ? WHERE id = ?", (now, now, user_id))
        conn.commit()
    return user_id


def change_password(user_id: int, new_password: str):
    new_hash, salt, iterations = _hash_password(new_password)
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    with _ensure_db() as conn:
        conn.execute(
            "UPDATE users SET password_hash = ?, salt = ?, iterations = ?, updated_at = ? WHERE id = ?",
            (new_hash, salt, iterations, now, user_id),
        )
        conn.commit()


def list_usernames():
    with _ensure_db() as conn:
        cur = conn.execute("SELECT username FROM users ORDER BY username ASC")
        return [row[0] for row in cur.fetchall()]
