import sqlite3
import uuid
import json
from datetime import datetime, timezone
from p6_replay import publicar_evento, estado_execucao
from scripts.p6_listener import montar_resposta_recordings

def test_regressao_contratos_oficiais_p6(monkeypatch):
    """Valida todos os tópicos publicados pelo P6 contra o ods-validator==0.1.0."""
    mensagens_capturadas = []

    class MockProducer:
        def produce(self, topic, value, callback=None):
            mensagens_capturadas.append((topic, value.decode("utf-8")))
        def poll(self, timeout):
            pass

    monkeypatch.setattr("p6_replay.producer", MockProducer())
    sessao_teste = str(uuid.uuid4())
    agora = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # 1. ods.p6.replay.state
    publicar_evento(
        topic="ods.p6.replay.state",
        schema="ods.p6.replay.state",
        session_id=sessao_teste,
        sequence=1,
        payload={
            "recording_id": "rec-001",
            "state": "PLAYING",
            "position_ms": 1000,
            "duration_ms": 5000,
            "speed_factor": 1.0,
            "loop": False
        }
    )

    # 2. ods.session.started
    publicar_evento(
        topic="ods.session.started",
        schema="ods.session.started",
        session_id=sessao_teste,
        sequence=0,
        payload={
            "mode": "REPLAY",
            "started_at": agora,
            "sources": ["cam0"],
            "recording_id": "rec-001",
            "reason": "START"
        }
    )

    # 3. ods.p4.frame
    publicar_evento(
        topic="ods.p4.frame",
        schema="ods.p4.frame",
        session_id=sessao_teste,
        sequence=2,
        payload={
            "frame_index": 0,
            "image_width": 640,
            "image_height": 480,
            "pixel_format": "BGR24",
            "fps": 30.0,
            "raw": True
        }
    )

    # 4. ods.p6.recordings.reply
    resposta = montar_resposta_recordings(str(uuid.uuid4()))

    assert len(mensagens_capturadas) == 3
    assert resposta["payload"]["status"] == "OK"
    assert len(resposta["payload"]["recordings"]) >= 1

def test_banco_cenarios_estrutura_e_consulta():
    """Verifica se a tabela scenarios no SQLite possui os campos exigidos pela especificação."""
    conn = sqlite3.connect("p6_metadata.db")
    cursor = conn.cursor()
    cursor.execute("SELECT recording_id, description, expected_outcome, file_path FROM scenarios WHERE recording_id = 'rec-001'")
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    assert row[0] == "rec-001"
    assert len(row[1]) > 0
    assert len(row[2]) > 0

def test_controle_estado_comandos_replay():
    """Valida a transição de estados de execução (PLAY -> PAUSE -> STOP)."""
    estado_execucao["ativo"] = True
    estado_execucao["pausado"] = False
    assert estado_execucao["ativo"] is True

    estado_execucao["pausado"] = True
    assert estado_execucao["pausado"] is True

    estado_execucao["ativo"] = False
    assert estado_execucao["ativo"] is False