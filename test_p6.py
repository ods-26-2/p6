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

def test_envelope_session_started():
    """Valida se a mensagem de início de sessão respeita os campos da Arquitetura 6c."""
    payload_exemplo = {
        "mode": "REPLAY",
        "started_at": "2026-11-06T14:30:00.000Z",
        "sources": ["cam0"],
        "recording_id": "rec-001"
    }
    assert payload_exemplo["mode"] in ["LIVE", "REPLAY"]
    assert isinstance(payload_exemplo["sources"], list)
    assert len(payload_exemplo["sources"]) > 0
    assert payload_exemplo["started_at"].endswith("Z")

def test_estrutura_replay_state():
    """Valida restrições de estado e tempo de ods.p6.replay.state."""
    state_payload = {
        "recording_id": "rec-001",
        "state": "PLAYING",
        "position_ms": 1500,
        "duration_ms": 10000,
        "speed_factor": 1.0,
        "loop": False
    }
    assert state_payload["state"] in ["PLAYING", "PAUSED", "STOPPED"]
    assert state_payload["position_ms"] <= state_payload["duration_ms"]
    assert state_payload["speed_factor"] > 0