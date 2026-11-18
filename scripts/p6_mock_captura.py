import cv2
import yt_dlp
import sqlite3
import uuid
from datetime import datetime, timezone

# 1. Preparar o Banco de Dados (SQLite)
conn = sqlite3.connect('../p6_metadata.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS frame_timestamps (
        session_id TEXT,
        frame_index INTEGER,
        media_timestamp TEXT,
        original_source TEXT
    )
''')
conn.commit()

# 2. Configurar o yt-dlp para extrair o stream
url_youtube = "https://www.youtube.com/watch?v=7aMOurgDB-o"
ydl_opts = {
    'format': 'bestvideo/best/b',
    'quiet': True
}

print("Resolvendo URL do stream...")
with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(url_youtube, download=False)
    stream_url = info['url']

cap = cv2.VideoCapture(stream_url)
fourcc = cv2.VideoWriter_fourcc(*'mp4v')
out = cv2.VideoWriter('../gravacao_p6.mp4', fourcc, 30.0, (640, 480))

# 3. Variáveis de estado da sessão
session_id = str(uuid.uuid4())
original_source = "P4_simulador"
frame_index = 0

print(f"Iniciando gravação. Sessão: {session_id}")

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    frame = cv2.resize(frame, (640, 480))
    
    # Gerando o timestamp no formato exigido pela interface
    timestamp_captura = datetime.now(timezone.utc).isoformat()

    # Inserindo o metadado no banco ANTES de gravar o quadro
    cursor.execute('''
        INSERT INTO frame_timestamps (session_id, frame_index, media_timestamp, original_source)
        VALUES (?, ?, ?, ?)
    ''', (session_id, frame_index, timestamp_captura, original_source))
    
    # Gravando o quadro físico do vídeo
    out.write(frame)
    
    frame_index += 1
    
    # Faz um commit a cada 30 quadros (aprox 1 segundo) para evitar perda de dados se o script quebrar
    if frame_index % 30 == 0:
        conn.commit()

    cv2.imshow('Monitoramento P6 - Gravacao (Aperte Q para sair)', frame)
    
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

# 4. Encerramento limpo
print(f"Gravação concluída. {frame_index} quadros salvos.")
conn.commit()
conn.close()
cap.release()
out.release()
cv2.destroyAllWindows()