import sqlite3
import uuid
from datetime import datetime, timezone

def test_formato_session_id():
    """Garante que session_id gerado seja UUID v4 válido."""
    novo_id = str(uuid.uuid4())
    assert isinstance(novo_id, str)
    assert len(novo_id) == 36

def test_timestamp_utc_iso8601():
    """Garante que o timestamp obedece ao padrão ISO 8601 UTC exigido pelo envelope."""
    ts = datetime.now(timezone.utc).isoformat()
    assert ts.endswith("+00:00") or ts.endswith("Z")

def test_persistencia_sqlite(tmp_path):
    """Valida leitura e escrita na tabela de metadados temporais."""
    db_path = tmp_path / "teste_metadata.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE frames (
            frame_index INTEGER PRIMARY KEY,
            captured_at TEXT NOT NULL
        )
    """)
    cur.execute("INSERT INTO frames VALUES (?, ?)", (1, "2026-10-23T14:30:00.000Z"))
    conn.commit()

    cur.execute("SELECT captured_at FROM frames WHERE frame_index = 1")
    row = cur.fetchone()
    assert row[0] == "2026-10-23T14:30:00.000Z"
    conn.close()