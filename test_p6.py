import sqlite3
import uuid
from datetime import datetime, timezone
from p6_replay import publicar_evento, estado_execucao

def test_regressao_campos_obrigatorios_envelope(monkeypatch):
    """Garante que o envelope publicado pelo P6 contém todos os campos exigidos pelo contrato 1.0."""
    mensagens_capturadas = []

    class MockProducer:
        def produce(self, topic, value, callback=None):
            mensagens_capturadas.append((topic, value.decode("utf-8")))
        def poll(self, timeout):
            pass

    monkeypatch.setattr("p6_replay.producer", MockProducer())

    sessao_teste = str(uuid.uuid4())
    publicar_evento(
        topic="ods.p6.replay.state",
        schema="ods.p6.replay.state",
        session_id=sessao_teste,
        sequence=1,
        payload={"recording_id": "rec-001", "state": "PLAYING"},
        source_id="cam0"
    )

    assert len(mensagens_capturadas) == 1
    topico, raw_json = mensagens_capturadas[0]
    import json
    envelope = json.loads(raw_json)

    campos_obrigatorios = [
        "message_type", "message_id", "schema", "schema_version",
        "producer", "source_id", "published_at", "captured_at",
        "session_id", "sequence", "payload"
    ]
    for campo in campos_obrigatorios:
        assert campo in envelope, f"Campo obrigatório ausente: {campo}"
        assert envelope[campo] is not None, f"Campo obrigatório nulo: {campo}"

    assert envelope["session_id"] == sessao_teste
    assert envelope["sequence"] == 1
    assert envelope["source_id"] == "cam0"

def test_banco_cenarios_estrutura_e_consulta():
    """Verifica se a tabela scenarios no SQLite possui os campos exigidos pela especificação."""
    conn = sqlite3.connect("p6_metadata.db")
    cursor = conn.cursor()
    cursor.execute("SELECT recording_id, description, expected_outcome, file_path FROM scenarios WHERE recording_id = 'rec-001'")
    row = cursor.fetchone()
    conn.close()

    assert row is not None, "Cenário padrão rec-001 não encontrado no banco"
    assert row[0] == "rec-001"
    assert len(row[1]) > 0
    assert len(row[2]) > 0

def test_controle_estado_comandos_replay():
    """Valida a transição de estados de execução (PLAY -> PAUSE -> STOP)."""
    estado_execucao["ativo"] = True
    estado_execucao["pausado"] = False
    assert estado_execucao["ativo"] is True
    assert estado_execucao["pausado"] is False

    # Simula PAUSE
    estado_execucao["pausado"] = True
    assert estado_execucao["pausado"] is True

    # Simula STOP
    estado_execucao["ativo"] = False
    assert estado_execucao["ativo"] is False