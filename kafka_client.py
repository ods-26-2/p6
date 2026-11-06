# kafka_client.py
import os
import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer, Consumer

DEFAULT_BROKERS = "172.18.4.56:9092,200.17.66.56:9092,34.66.235.91:9093"
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BROKERS)

def criar_produtor():
    return Producer({"bootstrap.servers": KAFKA_BROKER})

def criar_consumidor(grupo="p6-replay-group"):
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": grupo,
        "auto.offset.reset": "latest"
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