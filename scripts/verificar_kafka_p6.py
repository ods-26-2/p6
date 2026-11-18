import os
import json
from confluent_kafka import Consumer

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "34.66.235.91:9093")

# Configuração do consumidor conforme o Guia B2
consumidor = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "teste-auditoria-p6",  # Grupo exclusivo para este teste
    "auto.offset.reset": "latest"      # Ouve apenas eventos emitidos a partir de agora
})

# Subscrever os tópicos oficiais emitidos pelo P6
topicos = ["ods.session.started", "ods.p6.replay.state"]
consumidor.subscribe(topicos)

print(f"[*] Ligado a {KAFKA_BROKER}")
print(f"[*] A escutar os tópicos {topicos}... (Pressione Ctrl+C para parar)")

try:
    while True:
        # Espera até 1 segundo por novas mensagens
        msg = consumidor.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"[!] Erro no Kafka: {msg.error()}")
            continue

        # Descodificar JSON
        dados = json.loads(msg.value().decode("utf-8"))
        topico_origem = msg.topic()
        
        print(f"\n---> [EVENTO RECEBIDO DO TÓPICO: {topico_origem}]")
        print(f"Producer : {dados.get('producer')}")
        print(f"Schema   : {dados.get('schema')}")
        print(f"Session  : {dados.get('session_id')}")
        print(f"Sequence : {dados.get('sequence')}")
        print(f"Payload  : {json.dumps(dados.get('payload'), indent=2)}")

except KeyboardInterrupt:
    print("\n[*] A encerrar consumidor...")
finally:
    consumidor.close()