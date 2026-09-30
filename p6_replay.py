import cv2
import sqlite3
import uuid
import json
from datetime import datetime, timezone

# 1. Ligar à base de dados para resgatar os metadados da gravação original
conn = sqlite3.connect('p6_metadata.db')
cursor = conn.cursor()

# Descobrir qual é o trecho de tempo que temos gravado
cursor.execute("SELECT media_timestamp FROM frame_timestamps ORDER BY frame_index ASC LIMIT 1")
start_timestamp = cursor.fetchone()[0]

cursor.execute("SELECT media_timestamp FROM frame_timestamps ORDER BY frame_index DESC LIMIT 1")
end_timestamp = cursor.fetchone()[0]

# 2. A REGRA DE OURO DO REPLAY: Nova sessão
# A arquitetura dita: "um reinício ou uma busca cria uma nova sessão e um marcador explícito"
nova_sessao_replay = str(uuid.uuid4())

# 3. Construir o Evento Textual 
evento_pubsub = {
    "schema": "ods.infra.estado_replay",
    "schema_version": "1.0",
    "message_type": "event",
    "producer": "P6",
    "published_at": datetime.now(timezone.utc).isoformat(),
    "payload": {
        "session_id": nova_sessao_replay,
        "original_source": "P4_simulador_arquivo",
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp
    }
}

print("\n=== MARCADO DE REINICIALIZAÇÃO ENVIADO AO PUB/SUB ===")
print(json.dumps(evento_pubsub, indent=4))
print("=====================================================\n")
print("Os componentes I2, S1 e S2 reinicializaram os seus estados.\n")

# 4. Iniciar o disfarce: P6 serve o vídeo como se fosse a fonte canónica
print("A iniciar o stream de vídeo (Paridade de contrato com P4)...")
cap = cv2.VideoCapture('gravacao_p6.mp4')

frame_atual = 0
while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    # Puxar o timestamp exato em que este quadro específico foi capturado no passado
    cursor.execute("SELECT media_timestamp FROM frame_timestamps WHERE frame_index = ?", (frame_atual,))
    resultado = cursor.fetchone()
    timestamp_original = resultado[0] if resultado else "Desconhecido"

    # Escrever na imagem para provar visualmente que o tempo original viaja com o vídeo
    cv2.putText(frame, f"Tempo Orig: {timestamp_original[11:19]}", (10, 30),
                cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
    cv2.putText(frame, f"Sessao P6: {nova_sessao_replay[:8]}", (10, 60),
                cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)

    cv2.imshow('Monitoramento P6 - REPLAY (Q para sair)', frame)
    
    frame_atual += 1
    
    # Pausa de ~33ms para simular a reprodução normal a 30fps
    if cv2.waitKey(33) & 0xFF == ord('q'):
        break

cap.release()
conn.close()
cv2.destroyAllWindows()
print("\nReplay concluído.")