# P6 - Fonte de Replay (ODS TRA-1)

Módulo responsável por gerenciar a reprodução de mídias gravadas e coordenar as quebras de sessão via Apache Kafka de acordo com os contratos da arquitetura ODS 6c.

## Pré-requisitos
- Python 3.12+
- OpenCV / yt-dlp / confluent-kafka

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Execução

1. **Executar Testes Unitários:**
   ```bash
   pytest test_p6.py
   ```

2. **Gerar Mídia de Mock (opcional):**
   ```bash
   python p6_mock_captura.py
   ```

3. **Iniciar o Motor de Replay:**
   ```bash
   python p6_replay.py
   ```

4. **Monitorar Eventos no Kafka:**
   ```bash
   python verificar_kafka_p6.py
   ```