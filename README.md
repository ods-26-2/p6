# P6 - Fonte de Replay (ODS TRA-2)

Módulo responsável por armazenar cenários de teste, gravar fluxos de mídia e coordenar a reprodução de gravações e quebras de sessão via Apache Kafka, seguindo os contratos da arquitetura ODS 6c.

## Arquitetura e Conexão (Nuvem)

O serviço encontra-se implantado em contêiner Docker na Google Cloud Platform (GCP), conectado continuamente ao broker Kafka central da disciplina:

- **Broker Kafka (Público):** `200.17.66.56:9093` (rede interna/Jetson: `172.18.4.56:9092`)
- **Tópicos de Entrada (Consumidos pelo P6):**
  - `ods.p6.recordings.query` — Consulta a lista de cenários disponíveis no banco SQLite (`p6_metadata.db`).
  - `ods.p6.replay.command` — Inicia a reprodução de um cenário gravado (`recording_id`) para consumo dos módulos de visão/detecção.
  - `ods.p6.record.command` — Inicia ou encerra a gravação de um novo cenário de fluxo.
- **Tópicos de Saída (Publicados pelo P6):**
  - `ods.p6.recordings.response` — Retorna os metadados dos cenários disponíveis.
  - Eventos de controle de sessão e frames conforme o contrato ODS 6c.

## Pré-requisitos (Execução Local)

- Python 3.12+
- Dependências listadas em `requirements.txt` (`opencv-python`, `yt-dlp`, `confluent-kafka`, `pydantic`, `pytest`)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Execução Local e Testes

1. **Executar Testes Unitários:**
```bash
pytest test_p6.py
```

2. **Inicializar o Banco de Cenários (opcional — já incluso no repositório):**
```bash
python3 scripts/setup_db.py
```

3. **Gerar Mídia de Mock (opcional — já incluso em `gravacao_p6.mp4`):**
```bash
python3 scripts/p6_mock_captura.py
```

4. **Iniciar o Listener Principal do P6:**
```bash
python3 -u scripts/p6_listener.py
```

5. **Monitorar Eventos no Broker Kafka:**
```bash
python3 verificar_kafka_p6.py
```

## Deploy com Docker

A imagem já embute o banco SQLite (`p6_metadata.db`) e a mídia padrão (`gravacao_p6.mp4`) para execução autônoma:

```bash
docker build -t ods-p6 .
docker run -d --restart unless-stopped --name p6-service ods-p6
```