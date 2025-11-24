import os
import sqlite3
import json
import random
from typing import Any, Dict, List
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# DB_MODE
DB_MODE = os.getenv('DB_MODE', 'sqlite').lower()

# Postgres settings (used only if DB_MODE == 'postgres')
PG_HOST: str = os.getenv('PG_HOST', '')
PG_DBNAME: str = os.getenv('PG_DBNAME', 'postgres')
PG_USER: str = os.getenv('PG_USER', '')
PG_PASSWORD: str = os.getenv('PG_PASSWORD', '')
PG_PORT: str = os.getenv('PG_PORT', '5432')
PG_SSLMODE: str = os.getenv('PG_SSLMODE', 'require')

# SQLite settings (used when DB_MODE == 'sqlite')
SQLITE_PATH = os.getenv('SQLITE_PATH', 'chicago_local.db')


def _init_sqlite() -> None:
    """Create sqlite DB and `crimes` table if it doesn't exist."""
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS crimes (
                id TEXT PRIMARY KEY,
                case_number TEXT,
                date TEXT,
                block TEXT,
                iucr TEXT,
                primary_type TEXT,
                description TEXT,
                location_description TEXT,
                arrest INTEGER,
                domestic INTEGER,
                beat TEXT,
                district TEXT,
                ward TEXT,
                community_area TEXT,
                fbi_code TEXT,
                year INTEGER,
                updated_on TEXT,
                latitude REAL,
                longitude REAL,
                location TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


if DB_MODE == 'sqlite':
    _init_sqlite()
else:
    import psycopg2
    from psycopg2.extras import execute_values
    
    def _init_postgres() -> None:
        """Create the `crimes` table in Postgres if it doesn't exist."""
        conn = psycopg2.connect(
            host=PG_HOST,
            dbname=PG_DBNAME,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT,
            sslmode=PG_SSLMODE,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS crimes (
                        id TEXT PRIMARY KEY,
                        case_number TEXT,
                        date TIMESTAMPTZ,
                        block TEXT,
                        iucr TEXT,
                        primary_type TEXT,
                        description TEXT,
                        location_description TEXT,
                        arrest BOOLEAN,
                        domestic BOOLEAN,
                        beat TEXT,
                        district TEXT,
                        ward TEXT,
                        community_area TEXT,
                        fbi_code TEXT,
                        year INTEGER,
                        updated_on TIMESTAMPTZ,
                        latitude DOUBLE PRECISION,
                        longitude DOUBLE PRECISION,
                        location TEXT
                    )
                    """
                )
            conn.commit()
        finally:
            conn.close()

    try:
        _init_postgres()
    except Exception:
        
        pass


def insert_crimes(records: List[Dict[str, Any]]) -> None:
    """Insert or update records in the configured backend."""
    if not records:
        return

    columns = [
        'id', 'case_number', 'date', 'block', 'iucr', 'primary_type',
        'description', 'location_description', 'arrest', 'domestic', 'beat',
        'district', 'ward', 'community_area', 'fbi_code', 'year', 'updated_on',
        'latitude', 'longitude', 'location'
    ]

    def _normalize_value(v: Any) -> Any:
        if v is None:
            return None
        
        try:
            
            if hasattr(v, 'to_pydatetime'):
                v = v.to_pydatetime()
        except Exception:
            pass

        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (list, dict)):
            try:
                return json.dumps(v)
            except Exception:
                return str(v)
        return v

    def _enforce_recent_date(rec: Dict[str, Any]) -> None:
        """Force record 'date' to be today's date and at least 1 hour earlier than now.

        Sets 'date' to UTC now minus between 1 hour and ~1 hour 59 minutes,
        updates 'updated_on' to now, and adjusts 'year'.
        """
        now = datetime.utcnow()
        extra_minutes = random.randint(0, 59)
        extra_seconds = random.randint(0, 59)
        new_date = now - timedelta(hours=1, minutes=extra_minutes, seconds=extra_seconds)
        rec['date'] = new_date
        rec['updated_on'] = now
        try:
            rec['year'] = new_date.year
        except Exception:
            rec['year'] = now.year

    for r in records:
        try:
            _enforce_recent_date(r)
        except Exception:
            pass

    if DB_MODE == 'sqlite':
        conn = sqlite3.connect(SQLITE_PATH)
        try:
            cur = conn.cursor()
            placeholders = ','.join('?' for _ in columns)
            insert_sql = f"INSERT OR REPLACE INTO crimes ({', '.join(columns)}) VALUES ({placeholders})"
            values = [tuple(_normalize_value(rec.get(col)) for col in columns) for rec in records]
            cur.executemany(insert_sql, values)
            conn.commit()
        finally:
            conn.close()
        return

    # Postgres 
    conn = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
        sslmode=PG_SSLMODE,
    )
    try:
        with conn.cursor() as cur:
            def _pg_norm(v: Any) -> Any:
                try:
                    if hasattr(v, 'to_pydatetime'):
                        v = v.to_pydatetime()
                except Exception:
                    pass
                if isinstance(v, datetime):
                    return v
                if isinstance(v, bool):
                    return v
                if isinstance(v, (list, dict)):
                    try:
                        return json.dumps(v)
                    except Exception:
                        return str(v)
                return v

            values = [tuple(_pg_norm(rec.get(col)) for col in columns) for rec in records]
            insert_sql = f"""
                INSERT INTO crimes ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                {', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col != 'id'])}
            """
            execute_values(cur, insert_sql, values)
        conn.commit()
    finally:
        conn.close()


def fetch_latest_crimes(limit: int = 5000) -> List[Dict[str, Any]]:
    columns = None
    if DB_MODE == 'sqlite':
        conn = sqlite3.connect(SQLITE_PATH)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM crimes ORDER BY date DESC LIMIT ?", (limit,))
            rows = cur.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    conn = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
        sslmode=PG_SSLMODE,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM crimes ORDER BY date DESC LIMIT %s", (limit,))
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def fetch_crime_by_id(crime_id: str) -> Dict[str, Any] | None:
    if DB_MODE == 'sqlite':
        conn = sqlite3.connect(SQLITE_PATH)
        try:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM crimes WHERE id = ?", (crime_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    conn = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
        sslmode=PG_SSLMODE,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM crimes WHERE id = %s", (crime_id,))
            row = cur.fetchone()
            if not row:
                return None
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))
    finally:
        conn.close()


def delete_crime_by_id(crime_id: str) -> bool:
    if DB_MODE == 'sqlite':
        conn = sqlite3.connect(SQLITE_PATH)
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM crimes WHERE id = ?", (crime_id,))
            deleted = cur.rowcount
            conn.commit()
            return deleted > 0
        finally:
            conn.close()

    conn = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
        sslmode=PG_SSLMODE,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM crimes WHERE id = %s", (crime_id,))
            deleted = cur.rowcount
        conn.commit()
        return deleted > 0
    finally:
        conn.close()
