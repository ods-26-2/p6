# kafka_client.py
import os
import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer

# Broker oficial da disciplina no Google Cloud 
DEFAULT_BROKER = "34.66.235.91:9093"

def obter_broker():
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    return broker if broker else DEFAULT_BROKER

def criar_produtor():
    return Producer({
        "bootstrap.servers": obter_broker(),
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 15000,
        "reconnect.backoff.ms": 500,
        "reconnect.backoff.max.ms": 5000
    })

def criar_consumidor(group_id):
    return Consumer({
        "bootstrap.servers": obter_broker(),
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "session.timeout.ms": 15000,
        "heartbeat.interval.ms": 3000
    })

def enviar_evento_ods(produtor, topico, schema, session_id, payload):
    """Monta o envelope comum de B1/B2 e publica no tópico."""
    envelope = {
        "message_type": "event",
        "message_id": str(uuid.uuid4()),
        "schema": schema,
        "schema_version": "1.0",
        "producer": "P6",
        "published_at": datetime.now(timezone.utc).isoformat(),
        "session_id": session_id,
        "payload": payload
    }
    produtor.produce(
        topic=topico,
        value=json.dumps(envelope).encode("utf-8")
    )
    produtor.flush()