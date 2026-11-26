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

estado_gravacao = {
    "gravando": False,
    "recording_id": None,
    "started_at": None
}
val = Validator()

def agora_utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def cenario_existe(recording_id):
    conn = sqlite3.connect("p6_metadata.db")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM scenarios WHERE recording_id = ?", (recording_id,))
    existe = cursor.fetchone() is not None
    conn.close()
    return existe

def iniciar_gravacao_simulada(recording_id, description, expected_outcome):
    print(f"[P6-REC] Gravando fluxo P4/P5 em '{recording_id}'...")
    
    conn = sqlite3.connect("p6_metadata.db")
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO scenarios (recording_id, description, expected_outcome, file_path)
        VALUES (?, ?, ?, ?)
    ''', (recording_id, description, expected_outcome, 'gravacao_p6.mp4'))
    conn.commit()
    conn.close()

    while estado_gravacao["gravando"] and estado_gravacao["recording_id"] == recording_id:
        time.sleep(0.5)
    print(f"[P6-REC] Gravação '{recording_id}' salva e indexada no SQLite.")

def montar_resposta_recordings(correlation_id, payload_query):
    conn = sqlite3.connect("p6_metadata.db")
    cursor = conn.cursor()
    cursor.execute("SELECT recording_id, description, expected_outcome FROM scenarios")
    linhas = cursor.fetchall()
    conn.close()

    filtro_label = (payload_query.get("label") or "").strip().lower()
    agora_iso = agora_utc()
    cenarios = []

    for l in linhas:
        label_completo = f"{l[1]} | Esperado: {l[2]}"
        if filtro_label and filtro_label not in label_completo.lower() and filtro_label not in l[0].lower():
            continue
        cenarios.append({
            "recording_id": l[0],
            "label": label_completo,
            "started_at": agora_iso,
            "ended_at": agora_iso,
            "sources": ["cam0"],
            "duration_ms": 5000
        })

    resposta = {
        "schema": "ods.p6.recordings.reply",
        "message_id": str(uuid.uuid4()),
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

def publicar_reply(produtor, topico, resposta):
    val.validate(resposta)
    produtor.produce(topico, value=json.dumps(resposta).encode('utf-8'))
    produtor.flush()

def tratar_record_command(produtor, corr_id, payload):
    acao = (payload.get("action") or "START").upper()

    if acao == "START":
        source_ids = payload.get("source_ids") or []
        if not source_ids:
            resposta = {
                "schema": "ods.p6.record.reply",
                "message_id": str(uuid.uuid4()),
                "message_type": "reply",
                "producer": "P6",
                "correlation_id": corr_id,
                "payload": {
                    "status": "ERROR",
                    "error": {
                        "code": "SOURCE_NOT_FOUND",
                        "message": "Nenhuma fonte válida informada em source_ids."
                    }
                }
            }
            publicar_reply(produtor, "ods.p6.record.reply", resposta)
            return

        if estado_gravacao["gravando"]:
            resposta = {
                "schema": "ods.p6.record.reply",
                "message_id": str(uuid.uuid4()),
                "message_type": "reply",
                "producer": "P6",
                "correlation_id": corr_id,
                "payload": {
                    "status": "ERROR",
                    "error": {
                        "code": "ALREADY_RECORDING",
                        "message": f"Já existe uma gravação ativa ({estado_gravacao['recording_id']})."
                    }
                }
            }
            publicar_reply(produtor, "ods.p6.record.reply", resposta)
            return

        rec_id = payload.get("recording_id") or f"rec-{uuid.uuid4().hex[:6]}"
        desc = payload.get("label") or payload.get("description") or "Cenário gravado sob demanda"
        expected = payload.get("expected_outcome", "Comportamento nominal esperado")
        inicio = agora_utc()

        estado_gravacao["gravando"] = True
        estado_gravacao["recording_id"] = rec_id
        estado_gravacao["started_at"] = inicio

        t = threading.Thread(target=iniciar_gravacao_simulada, args=(rec_id, desc, expected), daemon=True)
        t.start()

        resposta = {
            "schema": "ods.p6.record.reply",
            "message_id": str(uuid.uuid4()),
            "message_type": "reply",
            "producer": "P6",
            "correlation_id": corr_id,
            "payload": {
                "status": "OK",
                "recording_id": rec_id,
                "started_at": inicio,
                "ended_at": None
            }
        }
        publicar_reply(produtor, "ods.p6.record.reply", resposta)

    elif acao == "STOP":
        rec_id = payload.get("recording_id", "")
        if not estado_gravacao["gravando"] or (rec_id and rec_id != estado_gravacao["recording_id"]):
            resposta = {
                "schema": "ods.p6.record.reply",
                "message_id": str(uuid.uuid4()),
                "message_type": "reply",
                "producer": "P6",
                "correlation_id": corr_id,
                "payload": {
                    "status": "ERROR",
                    "error": {
                        "code": "RECORDING_NOT_FOUND",
                        "message": f"Gravação '{rec_id}' não está ativa ou não foi encontrada."
                    }
                }
            }
            publicar_reply(produtor, "ods.p6.record.reply", resposta)
            return

        inicio = estado_gravacao["started_at"] or agora_utc()
        fim = agora_utc()
        estado_gravacao["gravando"] = False
        estado_gravacao["recording_id"] = None

        resposta = {
            "schema": "ods.p6.record.reply",
            "message_id": str(uuid.uuid4()),
            "message_type": "reply",
            "producer": "P6",
            "correlation_id": corr_id,
            "payload": {
                "status": "OK",
                "recording_id": rec_id,
                "started_at": inicio,
                "ended_at": fim
            }
        }
        publicar_reply(produtor, "ods.p6.record.reply", resposta)

def montar_erro_replay(corr_id, code, message):
    return {
        "schema": "ods.p6.replay.reply",
        "message_id": str(uuid.uuid4()),
        "message_type": "reply",
        "producer": "P6",
        "correlation_id": corr_id,
        "payload": {
            "status": "ERROR",
            "error": {
                "code": code,
                "message": message
            }
        }
    }

def montar_ok_replay(corr_id, rec_id, state_str):
    sess_id = estado_execucao["session_id"] or str(uuid.uuid4())
    return {
        "schema": "ods.p6.replay.reply",
        "message_id": str(uuid.uuid4()),
        "message_type": "reply",
        "producer": "P6",
        "correlation_id": corr_id,
        "payload": {
            "status": "OK",
            "recording_id": rec_id,
            "session_id": sess_id,
            "state": state_str,
            "position_ms": max(0, int(estado_execucao["position_ms"])),
            "speed_factor": max(0.1, float(estado_execucao["speed_factor"])),
            "loop": bool(estado_execucao["loop"])
        }
    }

def tratar_replay_command(produtor, corr_id, payload):
    acao = (payload.get("action") or "START").upper()
    rec_id = payload.get("recording_id", "rec-001")

    if not cenario_existe(rec_id):
        publicar_reply(produtor, "ods.p6.replay.reply", montar_erro_replay(corr_id, "RECORDING_NOT_FOUND", f"Cenário '{rec_id}' não encontrado."))
        return

    if acao in ("START", "PLAY", "RESTART"):
        speed = float(payload.get("speed_factor", 1.0))
        if speed <= 0:
            publicar_reply(produtor, "ods.p6.replay.reply", montar_erro_replay(corr_id, "INVALID_POSITION", "speed_factor deve ser maior que zero."))
            return
        loop = bool(payload.get("loop", False))
        sess_id = str(uuid.uuid4())
        estado_execucao["session_id"] = sess_id
        estado_execucao["recording_id"] = rec_id
        estado_execucao["position_ms"] = 0
        estado_execucao["speed_factor"] = speed
        estado_execucao["loop"] = loop

        if not estado_execucao["ativo"] or acao == "RESTART":
            print(f"[REPLAY] Iniciando cenário {rec_id}...")
            t = threading.Thread(target=executar_replay, args=(rec_id, speed, loop, True, sess_id), daemon=True)
            t.start()
        elif estado_execucao["pausado"]:
            estado_execucao["pausado"] = False

        publicar_reply(produtor, "ods.p6.replay.reply", montar_ok_replay(corr_id, rec_id, "PLAYING"))

    elif acao == "RESUME":
        if not estado_execucao["ativo"]:
            publicar_reply(produtor, "ods.p6.replay.reply", montar_erro_replay(corr_id, "NOT_PLAYING", "Nenhuma sessão de replay ativa para retomar."))
            return
        estado_execucao["pausado"] = False
        publicar_reply(produtor, "ods.p6.replay.reply", montar_ok_replay(corr_id, rec_id, "PLAYING"))

    elif acao == "PAUSE":
        if not estado_execucao["ativo"] or estado_execucao["pausado"]:
            publicar_reply(produtor, "ods.p6.replay.reply", montar_erro_replay(corr_id, "NOT_PLAYING", "Nenhuma sessão de replay em reprodução para pausar."))
            return
        estado_execucao["pausado"] = True
        print("[REPLAY] Pausado.")
        publicar_reply(produtor, "ods.p6.replay.reply", montar_ok_replay(corr_id, rec_id, "PAUSED"))

    elif acao == "SEEK":
        pos = payload.get("position_ms", -1)
        if not isinstance(pos, int) or pos < 0:
            publicar_reply(produtor, "ods.p6.replay.reply", montar_erro_replay(corr_id, "INVALID_POSITION", "position_ms inválido (deve ser inteiro >= 0)."))
            return
        estado_execucao["seek_ms"] = pos
        estado_execucao["position_ms"] = pos
        estado_str = "PAUSED" if estado_execucao["pausado"] else ("PLAYING" if estado_execucao["ativo"] else "STOPPED")
        publicar_reply(produtor, "ods.p6.replay.reply", montar_ok_replay(corr_id, rec_id, estado_str))

    elif acao == "SPEED":
        speed = float(payload.get("speed_factor", 0))
        if speed <= 0:
            publicar_reply(produtor, "ods.p6.replay.reply", montar_erro_replay(corr_id, "INVALID_POSITION", "speed_factor deve ser maior que zero."))
            return
        estado_execucao["speed_factor"] = speed
        estado_str = "PAUSED" if estado_execucao["pausado"] else ("PLAYING" if estado_execucao["ativo"] else "STOPPED")
        publicar_reply(produtor, "ods.p6.replay.reply", montar_ok_replay(corr_id, rec_id, estado_str))

    elif acao == "LOOP":
        estado_execucao["loop"] = bool(payload.get("loop", False))
        estado_str = "PAUSED" if estado_execucao["pausado"] else ("PLAYING" if estado_execucao["ativo"] else "STOPPED")
        publicar_reply(produtor, "ods.p6.replay.reply", montar_ok_replay(corr_id, rec_id, estado_str))

    elif acao == "STOP":
        estado_execucao["ativo"] = False
        estado_execucao["pausado"] = False
        print("[REPLAY] Parando...")
        publicar_reply(produtor, "ods.p6.replay.reply", montar_ok_replay(corr_id, rec_id, "STOPPED"))

def iniciar_escuta():
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
            corr_id = dados.get("message_id", str(uuid.uuid4()))

            if topico == "ods.p6.recordings.query":
                resposta = montar_resposta_recordings(corr_id, payload)
                publicar_reply(produtor, "ods.p6.recordings.reply", resposta)
                print(f"[QUERY] Lista validada e enviada com {len(resposta['payload']['recordings'])} cenário(s).")

            elif topico == "ods.p6.replay.command":
                tratar_replay_command(produtor, corr_id, payload)

            elif topico == "ods.p6.record.command":
                tratar_record_command(produtor, corr_id, payload)

    except KeyboardInterrupt:
        print("\nEncerrando P6 Listener...")
    finally:
        consumidor.close()

if __name__ == "__main__":
    iniciar_escuta()