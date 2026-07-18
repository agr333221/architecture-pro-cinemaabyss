"""
CinemaAbyss Proxy Service (API Gateway).

Реализация паттерна Strangler Fig: единая точка входа, которая постепенно
переключает трафик с монолита на новые микросервисы с помощью фиче-флага
GRADUAL_MIGRATION и процента миграции MOVIES_MIGRATION_PERCENT.
"""

import logging
import os
import random

import requests
from flask import Flask, Response, request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] proxy-service: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

PORT = int(os.getenv("PORT", "8000"))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))

# Заголовки hop-by-hop не должны пересылаться проксёй (RFC 2616, раздел 13.5.1)
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}


def choose_movies_backend() -> str:
    """Выбирает бэкенд для /api/movies по правилам Strangler Fig.

    При включённом фиче-флаге GRADUAL_MIGRATION заданный процент запросов
    (MOVIES_MIGRATION_PERCENT) уходит в новый микросервис movies, остальные —
    в монолит. При выключенном флаге весь трафик movies идёт в микросервис.
    """
    if not GRADUAL_MIGRATION:
        return MOVIES_SERVICE_URL
    if random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
        return MOVIES_SERVICE_URL
    return MONOLITH_URL


def resolve_backend(path: str) -> str:
    if path.startswith("/api/events"):
        return EVENTS_SERVICE_URL
    if path == "/api/movies/health":
        # health-эндпоинт существует только в микросервисе movies
        return MOVIES_SERVICE_URL
    if path.startswith("/api/movies"):
        return choose_movies_backend()
    return MONOLITH_URL


def forward(backend: str, path: str) -> Response:
    url = backend + path
    headers = {
        k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS
    }
    try:
        upstream = requests.request(
            method=request.method,
            url=url,
            params=request.args,
            data=request.get_data(),
            headers=headers,
            timeout=30,
        )
    except requests.RequestException as exc:
        logger.error("Upstream request to %s failed: %s", url, exc)
        return Response(
            '{"error": "Bad Gateway: upstream service is unavailable"}',
            status=502,
            mimetype="application/json",
        )

    logger.info(
        "%s %s -> %s [%s]", request.method, path, backend, upstream.status_code
    )
    response_headers = [
        (k, v)
        for k, v in upstream.headers.items()
        if k.lower() not in HOP_BY_HOP_HEADERS
    ]
    return Response(upstream.content, status=upstream.status_code, headers=response_headers)


@app.route("/health", methods=["GET"])
def health() -> Response:
    return Response("Strangler Fig Proxy is healthy", status=200, mimetype="text/plain")


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
@app.route("/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def proxy(path: str) -> Response:
    full_path = "/" + path
    backend = resolve_backend(full_path)
    return forward(backend, full_path)


if __name__ == "__main__":
    logger.info(
        "Starting proxy on :%d (gradual_migration=%s, movies_migration_percent=%d%%)",
        PORT,
        GRADUAL_MIGRATION,
        MOVIES_MIGRATION_PERCENT,
    )
    app.run(host="0.0.0.0", port=PORT, threaded=True)
