"""
CinemaAbyss Events Service (MVP).

Событийно-ориентированный сервис на Kafka: при вызове API создаёт события
User/Payment/Movie (producer), сам же читает их из топиков (consumer)
и пишет обработку в лог сервиса.
"""

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, request
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] events-service: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

PORT = int(os.getenv("PORT", "8082"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")

TOPICS = {
    "movie": "movie-events",
    "user": "user-events",
    "payment": "payment-events",
}

REQUIRED_FIELDS = {
    "movie": ["movie_id", "title", "action"],
    "user": ["user_id", "action", "timestamp"],
    "payment": ["payment_id", "user_id", "amount", "status", "timestamp"],
}

_producer = None
_producer_lock = threading.Lock()


def get_producer() -> KafkaProducer:
    """Лениво создаёт KafkaProducer с повторными попытками, пока брокер не поднимется."""
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            logger.info("Kafka producer connected to %s", KAFKA_BROKERS)
        return _producer


def consume_events() -> None:
    """Фоновый consumer: читает все топики событий и логирует обработку."""
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPICS.values(),
                bootstrap_servers=KAFKA_BROKERS,
                group_id="events-service",
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            logger.info("Kafka consumer subscribed to topics: %s", list(TOPICS.values()))
            for message in consumer:
                logger.info(
                    "Processed event from topic=%s partition=%d offset=%d: %s",
                    message.topic,
                    message.partition,
                    message.offset,
                    json.dumps(message.value, ensure_ascii=False),
                )
        except NoBrokersAvailable:
            logger.warning("Kafka brokers %s not available yet, retrying in 5s", KAFKA_BROKERS)
            time.sleep(5)
        except Exception:
            logger.exception("Consumer error, restarting in 5s")
            time.sleep(5)


def build_event(event_type: str, payload: dict) -> dict:
    entity_id = payload.get(f"{event_type}_id", payload.get("user_id", uuid.uuid4().hex[:8]))
    action = payload.get("action", payload.get("status", "created"))
    return {
        "id": f"{event_type}-{entity_id}-{action}",
        "type": event_type,
        "timestamp": payload.get("timestamp") or datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def publish_event(event_type: str) -> Response:
    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({"error": "Invalid JSON body"}), 400

    missing = [f for f in REQUIRED_FIELDS[event_type] if f not in payload]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    event = build_event(event_type, payload)
    topic = TOPICS[event_type]
    try:
        future = get_producer().send(topic, event)
        metadata = future.get(timeout=15)
    except (KafkaError, NoBrokersAvailable) as exc:
        logger.error("Failed to publish event to %s: %s", topic, exc)
        return jsonify({"error": "Failed to publish event to Kafka"}), 500

    logger.info(
        "Produced %s event to topic=%s partition=%d offset=%d: %s",
        event_type,
        topic,
        metadata.partition,
        metadata.offset,
        json.dumps(event, ensure_ascii=False),
    )
    return (
        jsonify(
            {
                "status": "success",
                "partition": metadata.partition,
                "offset": metadata.offset,
                "event": event,
            }
        ),
        201,
    )


@app.route("/api/events/health", methods=["GET"])
def health():
    return jsonify({"status": True}), 200


@app.route("/api/events/movie", methods=["POST"])
def create_movie_event():
    return publish_event("movie")


@app.route("/api/events/user", methods=["POST"])
def create_user_event():
    return publish_event("user")


@app.route("/api/events/payment", methods=["POST"])
def create_payment_event():
    return publish_event("payment")


if __name__ == "__main__":
    threading.Thread(target=consume_events, daemon=True, name="kafka-consumer").start()
    logger.info("Starting events service on :%d (brokers=%s)", PORT, KAFKA_BROKERS)
    app.run(host="0.0.0.0", port=PORT, threaded=True)
