import os
import cv2
import sqlite3
import uuid
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
from ods_validator import Validator
from kafka_client import criar_produtor

DEFAULT_BROKERS = "34.66.235.91:9093,172.18.4.56:9092,200.17.66.56:9092"
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BROKERS)
producer = criar_produtor()
val = Validator()

estado_execucao = {
    "ativo": False,
    "pausado": False,
    "session_id": None,
    "recording_id": "rec-001",
    "position_ms": 0,
    "speed_factor": 1.0,
    "loop": False,
    "seek_ms": None
}

def entrega_callback(err, msg):
    if err is not None:
        print(f"[!] Falha na entrega para o Kafka: {err}")

def publicar_evento(topic, schema, session_id, sequence, payload, captured_at=None, source_id="cam0"):
    agora = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    if schema == "ods.session.started":
        envelope = {
            "schema": schema,
            "message_id": str(uuid.uuid4()),
            "message_type": "event",
            "producer": "P6",
            "payload": payload
        }
    else:
        envelope = {
            "message_type": "event",
            "message_id": str(uuid.uuid4()),
            "schema": schema,
            "schema_version": "1.0",
            "producer": "P6",
            "source_id": source_id,
            "published_at": agora,
            "captured_at": captured_at if captured_at else agora,
            "session_id": session_id,
            "sequence": sequence,
            "payload": payload
        }
        
    val.validate(envelope)
    producer.produce(
        topic=topic,
        value=json.dumps(envelope).encode("utf-8"),
        callback=entrega_callback
    )
    producer.poll(0)

def executar_replay(recording_id="rec-001", speed_factor=1.0, loop=False, headless=True, session_id=None):
    nova_sessao_replay = session_id or str(uuid.uuid4())
    estado_execucao["ativo"] = True
    estado_execucao["pausado"] = False
    estado_execucao["session_id"] = nova_sessao_replay
    estado_execucao["recording_id"] = recording_id
    estado_execucao["position_ms"] = 0
    estado_execucao["speed_factor"] = speed_factor
    estado_execucao["loop"] = loop
    estado_execucao["seek_ms"] = None

    conn = sqlite3.connect('p6_metadata.db')
    cursor = conn.cursor()

    cursor.execute("SELECT file_path FROM scenarios WHERE recording_id = ?", (recording_id,))
    row_video = cursor.fetchone()
    caminho_video = row_video[0] if row_video and os.path.exists(row_video[0]) else 'gravacao_p6.mp4'

    cursor.execute("SELECT COUNT(*) FROM frame_timestamps")
    res_count = cursor.fetchone()
    total_frames = res_count[0] if res_count and res_count[0] > 0 else 150

    sequencia_p6 = 0

    payload_sessao = {
        "mode": "REPLAY",
        "started_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sources": ["cam0"],
        "recording_id": recording_id,
        "reason": "START"
    }
    publicar_evento("ods.session.started", "ods.session.started", nova_sessao_replay, sequencia_p6, payload_sessao)
    sequencia_p6 += 1
    producer.flush()
    print(f"[P6] Sessão REPLAY iniciada ({recording_id}): {nova_sessao_replay}")

    cap = cv2.VideoCapture(caminho_video)
    fps = float(cap.get(cv2.CAP_PROP_FPS) or 30.0)
    largura = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 640)
    altura = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 480)
    duracao_total_ms = int((total_frames / fps) * 1000)
    frame_atual = 0
    ultimo_envio_estado = time.time()

    while cap.isOpened() and estado_execucao["ativo"]:
        if estado_execucao["seek_ms"] is not None:
            frame_alvo = int((estado_execucao["seek_ms"] / 1000.0) * fps)
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_alvo)
            frame_atual = frame_alvo
            estado_execucao["position_ms"] = estado_execucao["seek_ms"]
            estado_execucao["seek_ms"] = None

        if estado_execucao["pausado"]:
            time.sleep(0.2)
            continue

        ret, frame = cap.read()
        if not ret:
            if estado_execucao["loop"]:
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                frame_atual = 0
                continue
            break

        posicao_ms = int((frame_atual / fps) * 1000)
        estado_execucao["position_ms"] = posicao_ms

        cursor.execute("SELECT media_timestamp FROM frame_timestamps WHERE frame_index = ?", (frame_atual,))
        resultado = cursor.fetchone()
        timestamp_original = resultado[0] if resultado else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        payload_frame = {
            "frame_index": frame_atual,
            "image_width": max(1, largura),
            "image_height": max(1, altura),
            "pixel_format": "BGR24",
            "fps": fps,
            "raw": True
        }
        publicar_evento("ods.p4.frame", "ods.p4.frame", nova_sessao_replay, sequencia_p6, payload_frame, captured_at=timestamp_original)
        sequencia_p6 += 1

        tempo_atual = time.time()
        if tempo_atual - ultimo_envio_estado >= 1.0:
            payload_estado = {
                "recording_id": recording_id,
                "state": "PLAYING",
                "position_ms": posicao_ms,
                "duration_ms": duracao_total_ms,
                "speed_factor": estado_execucao["speed_factor"],
                "loop": estado_execucao["loop"]
            }
            publicar_evento("ods.p6.replay.state", "ods.p6.replay.state", nova_sessao_replay, sequencia_p6, payload_estado)
            sequencia_p6 += 1
            ultimo_envio_estado = tempo_atual

        vel = max(0.1, float(estado_execucao["speed_factor"]))
        if not headless:
            cv2.imshow('Monitoramento P6 - REPLAY (Q para sair)', frame)
            if cv2.waitKey(max(1, int((1000 / fps) / vel))) & 0xFF == ord('q'):
                break
        else:
            time.sleep(max(0.01, (1.0 / fps) / vel))

        frame_atual += 1

    payload_fim = {
        "recording_id": recording_id,
        "state": "STOPPED",
        "position_ms": estado_execucao["position_ms"],
        "duration_ms": duracao_total_ms,
        "speed_factor": estado_execucao["speed_factor"],
        "loop": estado_execucao["loop"]
    }
    publicar_evento("ods.p6.replay.state", "ods.p6.replay.state", nova_sessao_replay, sequencia_p6, payload_fim)
    producer.flush()

    cap.release()
    conn.close()
    if not headless:
        cv2.destroyAllWindows()
    estado_execucao["ativo"] = False
    estado_execucao["pausado"] = False
    print(f"[P6] Replay finalizado ({recording_id}).")

if __name__ == "__main__":
    executar_replay(headless=False)