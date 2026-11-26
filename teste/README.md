# Testes de integração P6 (Kafka)

Esta pasta contém o runner de integração e os CSVs gerados; os relatórios ficam
ao lado do script, separados do restante do projeto.

## Instalação

Na pasta `p6`:

```powershell
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements.txt
```

## Execução

Ainda na pasta `p6`, execute os dois ambientes:

```powershell
python teste\teste_integracao_p6.py --scenario both
```

Os resultados serão gravados nesta pasta:

- `resultados_teste_nuvem_google.csv`
- `resultados_teste_jetson.csv`

Cada planilha CSV contém exatamente: `passou`, `entrada_modelo`,
`saida_esperada`, `entrada_teste`, `resultado_saida`.
`saida_esperada` mostra o contrato de reply definido no HTML da arquitetura,
incluindo a correlação com `message_id`; campos gerados dinamicamente aparecem
como descritores entre `< >`.

## Configuração

Os padrões são nuvem Google `34.66.235.91:9093` e Jetson
`172.18.4.56:9092`. O endereço da Jetson é privado e só fica acessível quando
a máquina está na rede correspondente.
Para substituir os endereços e timeouts no PowerShell:

```powershell
$env:P6_CLOUD_HOST = "34.66.235.91"
$env:P6_CLOUD_PORT = "9093"
$env:P6_JETSON_HOST = "172.18.4.56"
$env:P6_JETSON_PORT = "9092"
$env:P6_TEST_TIMEOUT_MS = "5000"
$env:P6_SEND_TIMEOUT_MS = "10000"
$env:P6_SCENARIO_TIMEOUT_MS = "60000"
```

Para sobrescrever a localização dos relatórios, defina `P6_CLOUD_OUTPUT` e/ou
`P6_JETSON_OUTPUT`. Se o Kafka não responder, o runner registra o timeout/erro
no CSV e continua; `P6_SCENARIO_TIMEOUT_MS` limita o tempo total de cada alvo.

No terminal, o resumo diferencia falhas de conexão/entrega Kafka, ausência de
reply e respostas recebidas que não atendem ao contrato. Casos sem conexão são
identificados como falha de transporte, e não como falha funcional do P6.
