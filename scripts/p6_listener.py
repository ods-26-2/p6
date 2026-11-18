import json
import sqlite3
import uuid
import threading
import time
from datetime import datetime, timezone
import sys
from pathlib import Path
from ods_validator import Validator

sys.path.append(str(Path(__file__).resolve().parent.parent))
from kafka_client import criar_consumidor, criar_produtor
from p6_replay import executar_replay, estado_execucao

gravando = False
val = Validator()

def iniciar_gravacao_simulada(recording_id, description, expected_outcome):
    global gravando
    gravando = True
    print(f"[P6-REC] Gravando fluxo P4/P5 em '{recording_id}'...")
    
    conn = sqlite3.connect("p6_metadata.db")
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO scenarios (recording_id, description, expected_outcome, file_path)
        VALUES (?, ?, ?, ?)
    ''', (recording_id, description, expected_outcome, 'gravacao_p6.mp4'))
    conn.commit()
    conn.close()

    while gravando:
        time.sleep(0.5)
    print(f"[P6-REC] Gravação '{recording_id}' salva e indexada no SQLite.")

def montar_resposta_recordings(correlation_id):
    conn = sqlite3.connect("p6_metadata.db")
    cursor = conn.cursor()
    cursor.execute("SELECT recording_id, description, expected_outcome FROM scenarios")
    linhas = cursor.fetchall()
    conn.close()

    agora_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    cenarios = [
        {
            "recording_id": l[0],
            "label": f"{l[1]} | Esperado: {l[2]}",
            "started_at": agora_iso,
            "ended_at": agora_iso,
            "sources": ["cam0"],
            "duration_ms": 5000
        }
        for l in linhas
    ]
    resposta = {
        "schema": "ods.p6.recordings.reply",
        "message_type": "reply",
        "producer": "P6",
        "correlation_id": correlation_id,
        "payload": {
            "status": "OK",
            "recordings": cenarios
        }
    }
    val.validate(resposta)
    return resposta

def iniciar_escuta():
    global gravando
    consumidor = criar_consumidor("grupo-p6-listener")
    consumidor.subscribe([
        "ods.p6.recordings.query",
        "ods.p6.replay.command",
        "ods.p6.record.command"
    ])
    produtor = criar_produtor()

    print("P6 Listener ativo! Ouvindo query, replay.command e record.command...")

    try:
        while True:
            msg = consumidor.poll(1.0)
            if msg is None or msg.error():
                continue

            topico = msg.topic()
            dados = json.loads(msg.value().decode('utf-8'))
            payload = dados.get("payload", {})

            if topico == "ods.p6.recordings.query":
                corr_id = dados.get("message_id", str(uuid.uuid4()))
                resposta = montar_resposta_recordings(corr_id)
                produtor.produce("ods.p6.recordings.reply", value=json.dumps(resposta).encode('utf-8'))
                produtor.flush()
                print(f"[QUERY] Lista validada e enviada com {len(resposta['payload']['recordings'])} cenário(s).")

            elif topico == "ods.p6.replay.command":
                acao = payload.get("action", "PLAY").upper()
                rec_id = payload.get("recording_id", "rec-001")
                speed = float(payload.get("speed_factor", 1.0))
                loop = bool(payload.get("loop", False))

                if acao == "PLAY":
                    if estado_execucao["pausado"]:
                        estado_execucao["pausado"] = False
                        print(f"[REPLAY] Retomando {rec_id}...")
                    elif not estado_execucao["ativo"]:
                        print(f"[REPLAY] Iniciando cenário {rec_id}...")
                        t = threading.Thread(target=executar_replay, args=(rec_id, speed, loop, True), daemon=True)
                        t.start()
                elif acao == "PAUSE":
                    estado_execucao["pausado"] = True
                    print("[REPLAY] Pausado.")
                elif acao == "STOP":
                    estado_execucao["ativo"] = False
                    print("[REPLAY] Parando...")

            elif topico == "ods.p6.record.command":
                acao = payload.get("action", "START").upper()
                rec_id = payload.get("recording_id", f"rec-{uuid.uuid4().hex[:6]}")
                desc = payload.get("description", "Cenário gravado sob demanda")
                expected = payload.get("expected_outcome", "Comportamento nominal esperado")

                if acao == "START" and not gravando:
                    t = threading.Thread(target=iniciar_gravacao_simulada, args=(rec_id, desc, expected), daemon=True)
                    t.start()
                elif acao == "STOP" and gravando:
                    gravando = False

    except KeyboardInterrupt:
        print("\nEncerrando P6 Listener...")
    finally:
        consumidor.close()

if __name__ == "__main__":
    iniciar_escuta()