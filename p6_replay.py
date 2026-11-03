import os
import cv2
import sqlite3
import uuid
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

# Configuração do broker Kafka (TRA-1 Google Cloud por defeito)
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "34.66.235.91:9093")
producer = Producer({"bootstrap.servers": KAFKA_BROKER})

def entrega_callback(err, msg):
    if err is not None:
        print(f"[!] Falha na entrega para o Kafka: {err}")
    else:
        print(f"[✓] Mensagem entregue ao tópico {msg.topic()} [partição {msg.partition()}]")

def publicar_evento(topic, schema, session_id, sequence, payload, captured_at=None):
    agora = datetime.now(timezone.utc).isoformat()
    envelope = {
        "message_type": "event",
        "message_id": str(uuid.uuid4()),
        "schema": schema,
        "schema_version": "1.0",
        "producer": "P6",
        "published_at": agora,
        "captured_at": captured_at if captured_at else agora,
        "session_id": session_id,
        "sequence": sequence,
        "payload": payload
    }
    
    producer.produce(
        topic=topic,
        value=json.dumps(envelope).encode("utf-8"),
        callback=entrega_callback
    )
    producer.poll(0)

# 1. Ligar à base de dados para resgatar os metadados
conn = sqlite3.connect('p6_metadata.db')
cursor = conn.cursor()

cursor.execute("SELECT media_timestamp FROM frame_timestamps ORDER BY frame_index ASC LIMIT 1")
res_start = cursor.fetchone()
start_timestamp = res_start[0] if res_start else datetime.now(timezone.utc).isoformat()

cursor.execute("SELECT media_timestamp FROM frame_timestamps ORDER BY frame_index DESC LIMIT 1")
res_end = cursor.fetchone()
end_timestamp = res_end[0] if res_end else start_timestamp

cursor.execute("SELECT COUNT(*) FROM frame_timestamps")
total_frames = cursor.fetchone()[0]

# 2. Regra de Replay: Início de nova sessão
nova_sessao_replay = str(uuid.uuid4())
sequencia_p6 = 0
recording_id = "rec-mock-001"

# 3. Publicar marcador canónico ods.session.started
payload_sessao = {
    "mode": "REPLAY",
    "started_at": datetime.now(timezone.utc).isoformat(),
    "sources": ["camo"],
    "recording_id": recording_id,
    "previous_session_id": None,
    "reason": "START"
}

publicar_evento(
    topic="ods.session.started",
    schema="ods.session.started",
    session_id=nova_sessao_replay,
    sequence=sequencia_p6,
    payload=payload_sessao
)
sequencia_p6 += 1
producer.flush()

print(f"[P6] Sessão iniciada no Kafka (tópico: ods.session.started): {nova_sessao_replay}")

# 4. Iniciar reprodução do vídeo
cap = cv2.VideoCapture('gravacao_p6.mp4')
fps = cap.get(cv2.CAP_PROP_FPS)
if fps <= 0:
    fps = 30.0

duracao_total_ms = int((total_frames / fps) * 1000)
frame_atual = 0
ultimo_envio_estado = time.time()

print("[P6] A transmitir vídeo e a emitir estado no Kafka...")

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    # Resgatar timestamp original
    cursor.execute("SELECT media_timestamp FROM frame_timestamps WHERE frame_index = ?", (frame_atual,))
    resultado = cursor.fetchone()
    timestamp_original = resultado[0] if resultado else "Desconhecido"

    # Publicar ods.p6.replay.state a cada 1 segundo
    tempo_atual = time.time()
    if tempo_atual - ultimo_envio_estado >= 1.0:
        posicao_ms = int((frame_atual / fps) * 1000)
        payload_estado = {
            "recording_id": recording_id,
            "state": "PLAYING",
            "position_ms": posicao_ms,
            "duration_ms": duracao_total_ms,
            "speed_factor": 1.0,
            "loop": False
        }
        publicar_evento(
            topic="ods.p6.replay.state",
            schema="ods.p6.replay.state",
            session_id=nova_sessao_replay,
            sequence=sequencia_p6,
            payload=payload_estado
        )
        sequencia_p6 += 1
        ultimo_envio_estado = tempo_atual

    # Sobreposição visual
    cv2.putText(frame, f"Tempo Orig: {timestamp_original[11:19]}", (10, 30),
                cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
    cv2.putText(frame, f"Sessao P6: {nova_sessao_replay[:8]}", (10, 60),
                cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)

    cv2.imshow('Monitoramento P6 - REPLAY (Q para sair)', frame)
    frame_atual += 1

    if cv2.waitKey(int(1000 / fps)) & 0xFF == ord('q'):
        break

# Enviar estado STOPPED ao encerrar
payload_fim = {
    "recording_id": recording_id,
    "state": "STOPPED",
    "position_ms": int((frame_atual / fps) * 1000),
    "duration_ms": duracao_total_ms,
    "speed_factor": 1.0,
    "loop": False
}
publicar_evento(
    topic="ods.p6.replay.state",
    schema="ods.p6.replay.state",
    session_id=nova_sessao_replay,
    sequence=sequencia_p6,
    payload=payload_fim
)
producer.flush()

cap.release()
conn.close()
cv2.destroyAllWindows()
print("[P6] Replay concluído e mensagens descarregadas no Kafka.")