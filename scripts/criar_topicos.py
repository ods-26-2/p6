# criar_topicos.py
import os
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "34.66.235.91:9093")
admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})

topicos_necessarios = [
    NewTopic("ods.session.started", num_partitions=1, replication_factor=1),
    NewTopic("ods.p6.replay.state", num_partitions=1, replication_factor=1)
]

print(f"[*] Tentando criar tópicos em {KAFKA_BROKER}...")
fs = admin.create_topics(topicos_necessarios)

for topico, f in fs.items():
    try:
        f.result()
        print(f"[✓] Tópico '{topico}' criado com sucesso.")
    except Exception as e:
        print(f"[!] Erro ao criar '{topico}': {e}")