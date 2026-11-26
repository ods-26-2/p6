import argparse
import csv
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer


COLUMNS = ["passou", "entrada_modelo", "saida_esperada", "entrada_teste", "resultado_saida"]

SCENARIOS = {
    "cloud": {
        "host": "34.66.235.91",
        "port": 9093,
        "output": "resultados_teste_nuvem_google.csv",
    },
    "jetson": {
        "host": "172.18.4.56",
        "port": 9092,
        "output": "resultados_teste_jetson.csv",
    },
}


def envelope(schema, message_type, payload, target="P6"):
    return {
        "message_type": message_type,
        "message_id": str(uuid.uuid4()),
        "schema": schema,
        "schema_version": "1.0",
        "producer": "B4",
        "target": target,
        "published_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "payload": payload,
    }


def compact(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def reply_contract(schema, correlation_id, payload):
    return {
        "message_type": "reply",
        "message_id": "<UUID gerado por P6>",
        "schema": schema,
        "schema_version": "1.0",
        "producer": "P6",
        "published_at": "<data/hora ISO 8601 UTC>",
        "correlation_id": correlation_id,
        "payload": payload,
    }


def is_error_response(response, allowed_codes=()):
    error = response.get("error")
    return (
        response.get("status") == "ERROR"
        and isinstance(error, dict)
        and isinstance(error.get("code"), str)
        and bool(error["code"])
        and isinstance(error.get("message"), str)
        and (not allowed_codes or error["code"] in allowed_codes)
    )


def is_recordings_response(response):
    recordings = response.get("recordings")
    if response.get("status") != "OK" or not isinstance(recordings, list):
        return False
    for recording in recordings:
        if not isinstance(recording, dict):
            return False
        if not all(
            isinstance(recording.get(field), str)
            for field in ("recording_id", "label", "started_at")
        ):
            return False
        if "ended_at" not in recording:
            return False
        if recording["ended_at"] is not None and not isinstance(recording["ended_at"], str):
            return False
        if not isinstance(recording.get("sources"), list):
            return False
        if not all(isinstance(source, str) for source in recording["sources"]):
            return False
        duration = recording.get("duration_ms")
        if not isinstance(duration, int) or isinstance(duration, bool) or duration < 0:
            return False
    return True


def is_recording_start_response(response):
    return (
        response.get("status") == "OK"
        and isinstance(response.get("recording_id"), str)
        and bool(response["recording_id"])
        and isinstance(response.get("started_at"), str)
        and response.get("ended_at") is None
    )


def is_replay_response(response, recording_id, state):
    position = response.get("position_ms")
    speed = response.get("speed_factor")
    return (
        response.get("status") == "OK"
        and response.get("recording_id") == recording_id
        and isinstance(response.get("session_id"), str)
        and isinstance(position, int)
        and not isinstance(position, bool)
        and position >= 0
        and response.get("state") == state
        and isinstance(speed, (int, float))
        and not isinstance(speed, bool)
        and speed > 0
        and isinstance(response.get("loop"), bool)
    )


def wait_for_assignment(consumer, timeout_ms):
    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        msg = consumer.poll(min(0.25, max(0, deadline - time.monotonic())))
        if msg is not None and msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise RuntimeError(f"KAFKA_CONSUMER_ERROR: {msg.error()}")
        if consumer.assignment():
            return
    raise TimeoutError("KAFKA_CONSUMER_ASSIGNMENT_TIMEOUT")


def wait_reply(consumer, correlation_id, timeout_ms):
    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        msg = consumer.poll(min(0.25, max(0, deadline - time.monotonic())))
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            return {
                "status": "ERROR",
                "error": f"KAFKA_CONSUMER_ERROR: {msg.error()}",
            }
        try:
            data = json.loads(msg.value().decode("utf-8"))
        except Exception:
            continue
        if data.get("correlation_id") == correlation_id:
            return data
    return {"status": "TIMEOUT", "error": "NO_REPLY_WITHIN_TIMEOUT"}


def execute_case(
    producer, consumer, topic, reply_topic, model, expected, expected_payload,
    payload, schema, message_type, timeout_ms, send_timeout_ms
):
    request = envelope(schema, message_type, payload)
    delivery_errors = []
    try:
        producer.produce(
            topic,
            value=json.dumps(request).encode("utf-8"),
            callback=lambda error, message: delivery_errors.append(str(error)) if error else None,
        )
        pending = producer.flush(send_timeout_ms / 1000)
        if pending:
            response = {
                "status": "TIMEOUT",
                "error": f"SEND_TIMEOUT: {pending} message(s) not delivered",
            }
        elif delivery_errors:
            response = {"status": "ERROR", "error": delivery_errors[0]}
        else:
            response = wait_reply(consumer, request["message_id"], timeout_ms)
    except Exception as exc:
        response = {
            "status": "ERROR",
            "error": f"KAFKA_PUBLISH_ERROR: {type(exc).__name__}: {exc}",
        }
    response_payload = response.get("payload", response)
    passed = expected(response_payload) if callable(expected) else str(expected) in compact(response_payload)
    error = str(response.get("error", ""))
    if passed:
        category = "passou"
    elif error.startswith(("SEND_TIMEOUT:", "KAFKA_")):
        category = "conexao"
        response["diagnostico"] = "Falha de conexão/entrega Kafka; não é resultado funcional do componente."
    elif response.get("status") == "TIMEOUT":
        category = "sem_resposta"
        response["diagnostico"] = "Mensagem enviada, mas não chegou reply correlacionada no prazo."
    else:
        category = "resposta_incorreta"
        response["diagnostico"] = "O componente respondeu, mas a resposta não atende ao contrato esperado."
    return {
        "passou": "SIM" if passed else "NAO",
        "entrada_modelo": compact(model),
        "saida_esperada": compact(reply_contract(
            reply_topic, request["message_id"], expected_payload
        )),
        "entrada_teste": compact(request),
        "resultado_saida": compact(response),
        "_classificacao": category,
    }


def run_scenario(name, config):
    host = os.getenv(f"P6_{name.upper()}_HOST", config["host"])
    port = int(os.getenv(f"P6_{name.upper()}_PORT", config["port"]))
    output = os.getenv(f"P6_{name.upper()}_OUTPUT", config["output"])
    broker = os.getenv(
        "P6_CLOUD_BROKER" if name == "cloud" else "P6_JETSON_BROKER",
        f"{host}:{port}",
    )
    timeout_ms = int(os.getenv("P6_TEST_TIMEOUT_MS", "5000"))
    send_timeout_ms = int(os.getenv("P6_SEND_TIMEOUT_MS", "10000"))
    connection_timeout_ms = 10000
    scenario_timeout_ms = int(os.getenv("P6_SCENARIO_TIMEOUT_MS", "60000"))

    cases = [
        (
            "ods.p6.recordings.query",
            "ods.p6.recordings.reply",
            {"from_at": None, "to_at": None, "label": None},
            is_recordings_response,
            {
                "status": "OK",
                "recordings": "<lista, possivelmente vazia>",
                "item_schema": {
                    "recording_id": "<string>",
                    "label": "<string>",
                    "started_at": "<data/hora ISO 8601 UTC>",
                    "ended_at": "<data/hora ISO 8601 UTC ou null>",
                    "sources": ["<source_id>"],
                    "duration_ms": "<inteiro>",
                },
            },
            {},
            "ods.p6.recordings.query",
            "query",
        ),
        (
            "ods.p6.recordings.query",
            "ods.p6.recordings.reply",
            {"from_at": None, "to_at": None, "label": "epi"},
            is_recordings_response,
            {
                "status": "OK",
                "recordings": "<lista filtrada por label; pode estar vazia>",
                "item_schema": {
                    "recording_id": "<string>",
                    "label": "<string>",
                    "started_at": "<data/hora ISO 8601 UTC>",
                    "ended_at": "<data/hora ISO 8601 UTC ou null>",
                    "sources": ["<source_id>"],
                    "duration_ms": "<inteiro>",
                },
            },
            {"label": "epi"},
            "ods.p6.recordings.query",
            "query",
        ),
        (
            "ods.p6.record.command",
            "ods.p6.record.reply",
            {"action": "START", "source_ids": ["cam0", "cam1", "mic0"], "label": "cenario-epi-03"},
            is_recording_start_response,
            {
                "status": "OK",
                "recording_id": "<string gerada por P6>",
                "started_at": "<data/hora ISO 8601 UTC>",
                "ended_at": None,
            },
            {"action": "START", "source_ids": ["cam0", "cam1", "mic0"], "label": "cenario-epi-03"},
            "ods.p6.record.command",
            "command",
        ),
        (
            "ods.p6.record.command",
            "ods.p6.record.reply",
            {"action": "START", "source_ids": ["cam0", "cam1", "mic0"], "label": "cenario-epi-03"},
            lambda r: is_error_response(
                r, ("SOURCE_NOT_FOUND", "ALREADY_RECORDING", "DISK_FULL")
            ),
            {
                "status": "ERROR",
                "error": {
                    "code": "SOURCE_NOT_FOUND | ALREADY_RECORDING | DISK_FULL",
                    "message": "<descrição do erro>",
                },
            },
            {"action": "START", "source_ids": []},
            "ods.p6.record.command",
            "command",
        ),
        (
            "ods.p6.record.command",
            "ods.p6.record.reply",
            {"action": "STOP", "recording_id": "rec-2026-09-23-001"},
            lambda r: is_error_response(r, ("RECORDING_NOT_FOUND",)),
            {
                "status": "ERROR",
                "error": {
                    "code": "RECORDING_NOT_FOUND",
                    "message": "<descrição do erro>",
                },
            },
            {"action": "STOP", "recording_id": "rec-nao-existente"},
            "ods.p6.record.command",
            "command",
        ),
        (
            "ods.p6.replay.command",
            "ods.p6.replay.reply",
            {"action": "START", "recording_id": "rec-2026-09-23-001"},
            lambda r: is_replay_response(r, "rec-001", "PLAYING"),
            {
                "status": "OK",
                "recording_id": "rec-001",
                "session_id": "<UUID de nova sessão>",
                "state": "PLAYING",
                "position_ms": "<inteiro >= 0>",
                "speed_factor": "<número > 0>",
                "loop": "<boolean>",
            },
            {"action": "START", "recording_id": "rec-001"},
            "ods.p6.replay.command",
            "command",
        ),
        (
            "ods.p6.replay.command",
            "ods.p6.replay.reply",
            {"action": "SEEK", "recording_id": "rec-2026-09-23-001", "position_ms": 125000},
            lambda r: is_error_response(
                r, ("INVALID_POSITION", "RECORDING_NOT_FOUND")
            ),
            {
                "status": "ERROR",
                "error": {
                    "code": "INVALID_POSITION | RECORDING_NOT_FOUND",
                    "message": "<descrição do erro>",
                },
            },
            {"action": "SEEK", "recording_id": "rec-001", "position_ms": -1},
            "ods.p6.replay.command",
            "command",
        ),
        (
            "ods.p6.replay.command",
            "ods.p6.replay.reply",
            {"action": "SPEED", "recording_id": "rec-2026-09-23-001", "speed_factor": 1.0},
            lambda r: is_error_response(r),
            {
                "status": "ERROR",
                "error": {
                    "code": "<código não especificado no HTML para speed_factor <= 0>",
                    "message": "<descrição do erro>",
                },
            },
            {"action": "SPEED", "recording_id": "rec-001", "speed_factor": 0},
            "ods.p6.replay.command",
            "command",
        ),
        (
            "ods.p6.replay.command",
            "ods.p6.replay.reply",
            {"action": "PAUSE", "recording_id": "rec-2026-09-23-001"},
            lambda r: is_error_response(
                r, ("NOT_PLAYING", "RECORDING_NOT_FOUND")
            ),
            {
                "status": "ERROR",
                "error": {
                    "code": "NOT_PLAYING | RECORDING_NOT_FOUND",
                    "message": "<descrição do erro>",
                },
            },
            {"action": "PAUSE", "recording_id": "rec-inexistente"},
            "ods.p6.replay.command",
            "command",
        ),
        (
            "ods.p6.replay.command",
            "ods.p6.replay.reply",
            {"action": "STOP", "recording_id": "rec-2026-09-23-001"},
            lambda r: is_replay_response(r, "rec-001", "STOPPED"),
            {
                "status": "OK",
                "recording_id": "rec-001",
                "session_id": "<UUID da sessão de replay>",
                "state": "STOPPED",
                "position_ms": "<inteiro >= 0>",
                "speed_factor": "<número > 0>",
                "loop": "<boolean>",
            },
            {"action": "STOP", "recording_id": "rec-001"},
            "ods.p6.replay.command",
            "command",
        ),
    ]

    rows = []
    configured_output = os.getenv(f"P6_{name.upper()}_OUTPUT")
    output_path = (
        Path(configured_output)
        if configured_output
        else Path(__file__).resolve().parent / output
    )
    deadline = time.monotonic() + scenario_timeout_ms / 1000
    producer = None
    consumer = None

    try:
        kafka_config = {
            "bootstrap.servers": broker,
            "security.protocol": "PLAINTEXT",
            "socket.timeout.ms": connection_timeout_ms,
            "socket.connection.setup.timeout.ms": connection_timeout_ms,
            "message.timeout.ms": send_timeout_ms,
            "reconnect.backoff.ms": 250,
            "reconnect.backoff.max.ms": 1000,
        }
        producer = Producer(kafka_config)
        consumer = Consumer({
            **kafka_config,
            "group.id": f"p6-integration-{name}-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([
            "ods.p6.recordings.reply",
            "ods.p6.record.reply",
            "ods.p6.replay.reply",
            "ods.p6.replay.state",
        ])
        wait_for_assignment(consumer, connection_timeout_ms)

        for index, case in enumerate(cases):
            (
                topic, reply_topic, model, expected, expected_payload, payload,
                schema, message_type,
            ) = case
            remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
            if remaining_ms <= 0:
                response = {"status": "TIMEOUT", "error": "SCENARIO_TIMEOUT"}
                request = envelope(schema, message_type, payload)
                rows.append({
                    "passou": "NAO",
                    "entrada_modelo": compact(model),
                    "saida_esperada": compact(reply_contract(
                        reply_topic, request["message_id"], expected_payload
                    )),
                    "entrada_teste": compact(request),
                    "resultado_saida": compact(response),
                    "_classificacao": "tempo_esgotado",
                })
                continue
            rows.append(execute_case(
                producer, consumer, topic, reply_topic, model, expected,
                expected_payload, payload, schema, message_type,
                min(timeout_ms, remaining_ms),
                min(send_timeout_ms, remaining_ms),
            ))
    except Exception as exc:
        error = {
            "status": "ERROR",
            "error": f"KAFKA_SETUP_ERROR: {type(exc).__name__}: {exc}",
            "diagnostico": "Falha ao iniciar a conexão/configuração Kafka; casos não executados.",
        }
        for case in cases[len(rows):]:
            (
                _, reply_topic, model, _, expected_payload, payload, schema,
                message_type,
            ) = case
            request = envelope(schema, message_type, payload)
            rows.append({
                "passou": "NAO",
                "entrada_modelo": compact(model),
                "saida_esperada": compact(reply_contract(
                    reply_topic, request["message_id"], expected_payload
                )),
                "entrada_teste": compact(request),
                "resultado_saida": compact(error),
                "_classificacao": "conexao",
            })
    finally:
        if consumer is not None:
            consumer.close()
        if producer is not None:
            producer.flush(0)
        with output_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(
                {column: row[column] for column in COLUMNS}
                for row in rows
            )

    counts = {
        category: sum(row.get("_classificacao") == category for row in rows)
        for category in (
            "passou", "conexao", "sem_resposta", "resposta_incorreta",
            "tempo_esgotado",
        )
    }
    print(
        f"[P6] {name} ({broker}): {counts['passou']} passaram; "
        f"{counts['conexao']} falhas de conexão Kafka; "
        f"{counts['sem_resposta']} sem reply; "
        f"{counts['resposta_incorreta']} respostas fora do esperado; "
        f"{counts['tempo_esgotado']} casos não executados pelo limite total."
    )
    if counts["conexao"]:
        print(
            "[P6] Falha de conexão/entrega Kafka: esses casos não permitem "
            "concluir que o componente respondeu incorretamente."
        )
    if counts["sem_resposta"]:
        print(
            "[P6] Sem reply correlacionada: conexão pode ter funcionado, mas "
            "o componente não respondeu no prazo ou a reply não chegou."
        )
    if counts["resposta_incorreta"]:
        print(
            "[P6] O componente respondeu, mas uma ou mais respostas não "
            "correspondem ao contrato esperado."
        )
    if counts["tempo_esgotado"]:
        print(
            "[P6] O limite total do cenário foi atingido; os casos restantes "
            "foram marcados como não executados."
        )
    print(f"[P6] Planilha: {output_path.resolve()}")


def main():
    parser = argparse.ArgumentParser(description="Testes Kafka de integração P6")
    parser.add_argument("--scenario", choices=["cloud", "jetson", "both"], default="both")
    args = parser.parse_args()

    names = ["cloud", "jetson"] if args.scenario == "both" else [args.scenario]
    for name in names:
        try:
            run_scenario(name, SCENARIOS[name])
        except Exception as exc:
            print(f"[P6] Falha ao preparar o cenário {name}: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
