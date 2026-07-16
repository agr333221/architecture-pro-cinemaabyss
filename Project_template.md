## Изучите [README.md](README.md) файл и структуру проекта.

## Задание 1

1. Спроектируйте to be архитектуру КиноБездны, разделив всю систему на отдельные
 домены и организовав интеграционное взаимодействие и единую точку вызова сервисов.
Результат представьте в виде контейнерной диаграммы в нотации С4.
Добавьте ссылку на файл в этот шаблон


Диаграмма контейнеров в нотации С4 (PlantUML): **[docs/schemas/containers.puml](docs/schemas/containers.puml)**,

![Диаграмма контейнеров ](docs/schemas/containers.svg)

Описание: [docs/to-be-architecture.md](docs/to-be-architecture.md))

**Решение.**
Система разделена на домены:
- Movies (вынесен в movies-service)
- Events (новый events-service на Kafka)
- Users/Payments/Subscriptions (пока в монолите, следующие кандидаты на выделение).
- Единая точка вызова — API Gateway (proxy-service, паттерн Strangler Fig):
  весь клиентский трафик идет через Ingress -> proxy-service, который постепенно
  переключает трафик с монолита на микросервисы по фиче-флагу `GRADUAL_MIGRATION`
  и проценту `MOVIES_MIGRATION_PERCENT`. Асинхронное интеграционное взаимодействие — через
  Kafka (топики `movie-events`, `user-events`, `payment-events`).

## Задание 2

### 1. Proxy
Команда КиноБездны уже выделила сервис метаданных о фильмах movies
 и вам необходимо реализовать бесшовный переход с применением
 паттерна Strangler Fig в части реализации прокси-сервиса (API Gateway),
 с помощью которого можно будет постепенно переключать траффик,
 используя фиче-флаг.


Реализуйте сервис на любом языке программирования в ./src/microservices/proxy.

Конфигурация для запуска сервиса через docker-compose уже добавлена
```yaml
  proxy-service:
    build:
      context: ./src/microservices/proxy
      dockerfile: Dockerfile
    container_name: cinemaabyss-proxy-service
    depends_on:
      - monolith
      - movies-service
      - events-service
    ports:
      - "8000:8000"
    environment:
      PORT: 8000
      MONOLITH_URL: http://monolith:8080
      #монолит
      MOVIES_SERVICE_URL: http://movies-service:8081 #сервис movies
      EVENTS_SERVICE_URL: http://events-service:8082 
      GRADUAL_MIGRATION: "true" # вкл/выкл простого фиче-флага
      MOVIES_MIGRATION_PERCENT: "50" # процент миграции
    networks:
      - cinemaabyss-network
```

- После реализации запустите postman тесты - они все должны быть зеленые.
- Отправьте запросы к API Gateway:
   ```bash
   curl http://localhost:8000/api/movies
   ```
- Протестируйте постепенный переход, изменив переменную окружения
 MOVIES_MIGRATION_PERCENT в файле docker-compose.yml.

**Решение.**
 Прокси-сервис реализован на Python (Flask + requests)
 в [src/microservices/proxy/app.py](src/microservices/proxy/app.py):

- `/health` — health-check прокси (200, `Strangler Fig Proxy is healthy`);
- `/api/movies*` — при `GRADUAL_MIGRATION=true` случайные `MOVIES_MIGRATION_PERCENT`% запросов
  уходят в movies-service, остальные — в монолит; при выключенном флаге весь трафик movies
  идет в микросервис; `/api/movies/health` всегда маршрутизируется в movies-service;
- `/api/events*` — проксируется в events-service;
- все остальные пути (`/api/users`, `/api/payments`, `/api/subscriptions`, …) — в монолит.

Прокси пересылает метод, query-параметры,
 тело и заголовки (кроме hop-by-hop),
 при недоступности бэкенда возвращает 502.

 Результаты проверки:

- postman-тесты: **22 запроса, 42 assertions, 0 ошибок** (все зеленые, включая events)
— лог прогона: [docs/postman-local-tests.txt](docs/postman-local-tests.txt)

- `curl http://localhost:8000/api/movies` возвращает список фильмов;
- при `MOVIES_MIGRATION_PERCENT=50` фактическое распределение 20+ запросов
  по логам прокси: ~50/50 между monolith и movies-service
  (проверено подсчетом строк `GET /api/movies -> <backend>` в логе),
  при `100` — весь трафик в movies-service.

### 2. Kafka
 Вам как архитектуру нужно также проверить гипотезу насколько
 просто реализовать применение Kafka в данной архитектуре.

Для этого нужно сделать MVP сервис events, который будет при вызове API создавать
 и сам же читать сообщения в топике Kafka.

    - Разработайте сервис на любом языке программирования с consumer'ами и producer'ами.
    - Реализуйте простой API, при вызове которого будут создаваться события User/Payment/Movie и обрабатываться внутри сервиса с записью в лог
    - Добавьте в docker-compose новый сервис, kafka там уже есть

Необходимые тесты для проверки этого API вызываются при запуске npm run test:local из папки tests/postman 

!!! Приложите скриншот тестов и скриншот состояния топиков Kafka http://localhost:8090 

**Решение.**
 Events-сервис реализован на Python (Flask + kafka-python)
 в [src/microservices/events/app.py](src/microservices/events/app.py):

- producer: `POST /api/events/movie|user|payment` валидирует обязательные поля,
  формирует событие `{id, type, timestamp, payload}` и
  публикует его в соответствующий топик (`movie-events`, `user-events`, `payment-events`),
  в ответ возвращает 201 с `{status: "success", partition, offset, event}`
  согласно api-specification.yaml
- consumer: фоновый поток с consumer-group `events-service` читает все три топика
 и пишет обработку каждого события в лог сервиса (`Processed event from topic=... partition=... offset=...`)
- `GET /api/events/health` возвращает `{"status": true}`

Сервис уже был описан в docker-compose.yml
 (events-service, порт 8082, `KAFKA_BROKERS=kafka:9092`).
 Проверено: событие публикуется (partition=0, offset растет),
 consumer его обрабатывает и логирует.
 Все postman-тесты Events Microservice зеленые.

Скриншоты (тесты и топики Kafka из UI http://localhost:8090):

![Postman тесты](docs/screenshots/postman-tests.png)
![Kafka топики 1](docs/screenshots/kafka01.png)
![Kafka топики 2](docs/screenshots/kafka02.png)


## Задание 3

Команда начала переезд в Kubernetes для лучшего масштабирования и повышения надежности. 
Вам, как архитектору осталось самое сложное:
 - реализовать CI/CD для сборки прокси сервиса
 - реализовать необходимые конфигурационные файлы для переключения трафика.


### CI/CD

 В папке .github/worflows доработайте деплой новых сервисов proxy
 и events в docker-build-push.yml , чтобы api-tests при сборке отрабатывали
 корректно при отправке коммита в вашу новую ветку.

Нужно доработать 
```yaml
on:
  push:
    branches: [ main ]
    paths:
      - 'src/**'
      - '.github/workflows/docker-build-push.yml'
  release:
    types: [published]
```
и добавить необходимые шаги в блок

```yaml
jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    steps:
      - name: Checkout repository
        uses: actions/checkout@v3

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v2

      - name: Log in to the Container registry
        uses: docker/login-action@v2
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

```

Как только сборка отработает и в github registry появятся
 ваши образы, можно переходить к блоку настройки Kubernetes
Успешным результатом данного шага является "зеленая" сборка и "зеленые" тесты

**Решение.**

 В [.github/workflows/docker-build-push.yml](.github/workflows/docker-build-push.yml):

- триггер `push.branches` расширен до `[ main, cinema ]` — сборка запускается при коммите в ветку задания;
- добавлены шаги `Extract metadata` + `Build and push` для **events-service** (context `./src/microservices/events`) и **proxy-service** (context `./src/microservices/proxy`) по аналогии с monolith/movies — образы публикуются в `ghcr.io/<owner>/<repo>/events-service` и `.../proxy-service` с тегами `latest`, `sha`, имя ветки;
- в [.github/workflows/api-tests.yml](.github/workflows/api-tests.yml) триггер расширен до `[ main, master, cinema ]`; workflow поднимает весь стек через docker compose и прогоняет newman-тесты в контейнере — локальный прогон этого же сценария зеленый (22/22 запросов, 42/42 assertions).


### Proxy в Kubernetes

#### Шаг 1
Для деплоя в kubernetes необходимо залогиниться в docker registry Github'а.
1. Создайте Personal Access Token (PAT) https://github.com/settings/tokens . Создавайте class с правом read:packages
2. В src/kubernetes/*.yaml (event-service, monolith, movies-service и proxy-service)  отредактируйте путь до ваших образов 

```bash
 spec:
      containers:
      - name: events-service
        image: ghcr.io/ваш логин/имя репозитория/events-service:latest
```

3. Добавьте в секрет src/kubernetes/dockerconfigsecret.yaml в поле
```bash
 .dockerconfigjson: значение в base64 файла ~/.docker/config.json         	
```

4. Если в ~/.docker/config.json нет значения для аутентификации
```json
{
        "auths": {
                "ghcr.io": {
                       тут пусто
                }
        }
}
```
то выполните 

и добавьте

```json 
 "auth": "имя пользователя:токен в base64"
```

Чтобы получить значение в base64 можно выполнить команду
```bash
 echo -n ваш_логин:ваш_токен | base64
```

После заполнения config.json, также прогоните содержимое через base64

```bash
cat .docker/config.json | base64
```

и полученное значение добавляем в

```bash
 .dockerconfigjson: значение в base64 файла ~/.docker/config.json
```

#### Шаг 2

  Доработайте src/kubernetes/event-service.yaml и src/kubernetes/proxy-service.yaml

  - Необходимо создать Deployment и Service 
  - Доработайте ingress.yaml, чтобы можно было с помощью тестов проверить создание событий
  - Выполните дальшейшие шаги для поднятия кластера:

  1. Создайте namespace:
  ```bash
  kubectl apply -f src/kubernetes/namespace.yaml
  ```
  2. Создайте секреты и переменные
  ```bash
  kubectl apply -f src/kubernetes/configmap.yaml
  kubectl apply -f src/kubernetes/secret.yaml
  kubectl apply -f src/kubernetes/dockerconfigsecret.yaml
  kubectl apply -f src/kubernetes/postgres-init-configmap.yaml
  ```

  3. Разверните базу данных:
  ```bash
  kubectl apply -f src/kubernetes/postgres.yaml
  ```

  На этом этапе если вызвать команду
  ```bash
  kubectl -n cinemaabyss get pod
  ```
  Вы увидите

  NAME         READY   STATUS    
  postgres-0   1/1     Running   

  4. Разверните Kafka:
  ```bash
  kubectl apply -f src/kubernetes/kafka/kafka.yaml
  ```

  Проверьте, теперь должно быть запущено 3 пода, если что-то не так, то посмотрите логи

  ```bash
  kubectl -n cinemaabyss logs имя_пода (например - kafka-0)
  ```

  5. Разверните монолит:

  ```bash
  kubectl apply -f src/kubernetes/monolith.yaml
  ```

  6. Разверните микросервисы:

  ```bash
  kubectl apply -f src/kubernetes/movies-service.yaml
  kubectl apply -f src/kubernetes/events-service.yaml
  ```

  7. Разверните прокси-сервис:
  ```bash
  kubectl apply -f src/kubernetes/proxy-service.yaml
  ```

  После запуска и поднятия подов вывод команды 

  ```bash
  kubectl -n cinemaabyss get pod
  ```

  Будет наподобие такого

  NAME                              READY   STATUS    

  events-service-7587c6dfd5-6whzx   1/1     Running  

  kafka-0                           1/1     Running   

  monolith-8476598495-wmtmw         1/1     Running  

  movies-service-6d5697c584-4qfqs   1/1     Running  

  postgres-0                        1/1     Running  

  proxy-service-577d6c549b-6qfcv    1/1     Running  

  zookeeper-0                       1/1     Running 

  8. Добавим ingress

  - добавьте аддон
  ```bash
  minikube addons enable ingress
  ```
  ```bash
  kubectl apply -f src/kubernetes/ingress.yaml
  ```
  9. Добавьте в /etc/hosts
  127.0.0.1 cinemaabyss.example.com

  10. Вызовите
  ```bash
  minikube tunnel
  ```
  11. Вызовите https://cinemaabyss.example.com/api/movies
  Вы должны увидеть вывод списка фильмов
  Можно поэкспериментировать со значением   MOVIES_MIGRATION_PERCENT в src/kubernetes/configmap.yaml и убедится, что вызовы movies уходят полностью в новый сервис

  12. Запустите тесты из папки tests/postman
  ```bash
   npm run test:kubernetes
  ```
  Часть тестов с health-чек упадет, но создание событий отработает.
  Откройте логи event-service и сделайте скриншот обработки событий

**Решение (Шаг 2).** Доработаны манифесты:

- [src/kubernetes/proxy-service.yaml](src/kubernetes/proxy-service.yaml) — Deployment (образ `proxy-service:latest`, порт 8000, env из `cinemaabyss-config` + `EVENTS_SERVICE_URL`, probes на `/health`, `imagePullSecrets: dockerconfigjson`) и Service (ClusterIP 8000);
- [src/kubernetes/events-service.yaml](src/kubernetes/events-service.yaml) — Deployment (образ `events-service:latest`, порт 8082, `KAFKA_BROKERS=kafka:9092`, probes на `/api/events/health`) и Service (ClusterIP 8082);
- [src/kubernetes/ingress.yaml](src/kubernetes/ingress.yaml) — добавлен маршрут `/` -> `proxy-service:8000` (единая точка входа, все запросы включая `/api/movies` идут через прокси); маршрут `/api/events` -> `events-service:8082` оставлен для прямой проверки создания событий тестами;
- [src/kubernetes/configmap.yaml](src/kubernetes/configmap.yaml) — добавлены `EVENTS_SERVICE_URL` и `KAFKA_BROKERS`.

Заменил путь к образам на `ghcr.io/agr3332211/architecture-pro-cinemaabyss/*` на путь своего репозитория
`dockerconfigsecret.yaml` - вставил PAT и добавил в .gitignore 
- `src/kubernetes/dockerconfigsecret.yaml`
- `src/kubernetes/helm/values.yaml`


 Дальше по шагам 1–12:
 namespace > configmap/secrets > postgres > kafka > monolith > микросервисы > proxy > ingress > minikube tunnel.

NB! Вместо 8:
```
kubectl get ingressclass
```

![Запуск](docs/screenshots/kuber01.png)

![pods](docs/screenshots/kuber02.png)




#### Шаг 3
Добавьте сюда скриншота вывода при вызове https://cinemaabyss.example.com/api/movies и  скриншот вывода event-service после вызова тестов.

![Вывод /api/movies](docs/screenshots/k8s-api-movies.png)

![Логи event-service](docs/screenshots/k8s-events-logs.png )

## Задание 4
Для простоты дальнейшего обновления и развертывания вам как архитектуру
 необходимо так же реализовать helm-чарты для прокси-сервиса и проверить работу 

Для этого:
1. Перейдите в директорию helm и отредактируйте файл values.yaml

```yaml
# Proxy service configuration
proxyService:
  enabled: true
  image:
    repository: ghcr.io/db-exp/cinemaabysstest/proxy-service
    tag: latest
    pullPolicy: Always
  replicas: 1
  resources:
    limits:
      cpu: 300m
      memory: 256Mi
    requests:
      cpu: 100m
      memory: 128Mi
  service:
    port: 80
    targetPort: 8000
    type: ClusterIP
```

- Вместо ghcr.io/db-exp/cinemaabysstest/proxy-service напишите свой путь до образа для всех сервисов
- для imagePullSecret проставьте свое значение (скопируйте из конфигурации kubernetes)
  ```yaml
  imagePullSecrets:
      dockerconfigjson: ewoJImF1dGhzIjogewoJCSJnaGNyLmlvIjogewoJCQkiYXV0aCI6ICJaR0l0Wlhod09tZG9jRjl2UTJocVZIa3dhMWhKVDIxWmFVZHJOV2hRUW10aFVXbFZSbTVaTjJRMFNYUjRZMWM9IgoJCX0KCX0sCgkiY3JlZHNTdG9yZSI6ICJkZXNrdG9wIiwKCSJjdXJyZW50Q29udGV4dCI6ICJkZXNrdG9wLWxpbnV4IiwKCSJwbHVnaW5zIjogewoJCSIteC1jbGktaGludHMiOiB7CgkJCSJlbmFibGVkIjogInRydWUiCgkJfQoJfSwKCSJmZWF0dXJlcyI6IHsKCQkiaG9va3MiOiAidHJ1ZSIKCX0KfQ==
  ```

2. В папке ./templates/services заполните шаблоны для proxy-service.yaml и events-service.yaml (опирайтесь на свою kubernetes конфигурацию - смысл helm'а сделать шаблоны для быстрого обновления и установки)

```yaml
template:
    metadata:
      labels:
        app: proxy-service
    spec:
      containers:
       Тут ваша конфигурация
```

3. Проверьте установку
Сначала удалим установку руками

```bash
kubectl delete all --all -n cinemaabyss
kubectl delete  namespace cinemaabyss
```
Запустите 
```bash
helm install cinemaabyss .\src\kubernetes\helm --namespace cinemaabyss --create-namespace
```
Если в процессе будет ошибка
```code
[2025-04-08 21:43:38,780] ERROR Fatal error during KafkaServer startup. Prepare to shutdown (kafka.server.KafkaServer)
kafka.common.InconsistentClusterIdException: The Cluster ID OkOjGPrdRimp8nkFohYkCw doesn't match stored clusterId Some(sbkcoiSiQV2h_mQpwy05zQ) in meta.properties. The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.
```

Проверьте развертывание:
```bash
kubectl get pods -n cinemaabyss
minikube tunnel
```

Потом вызовите 
https://cinemaabyss.example.com/api/movies
и приложите скриншот развертывания helm и вывода https://cinemaabyss.example.com/api/movies

**Решение.**

 Заполнены Helm-шаблоны в [src/kubernetes/helm/templates/services/](src/kubernetes/helm/templates/services/):

- [proxy-service.yaml](src/kubernetes/helm/templates/services/proxy-service.yaml) — Deployment (образ/теги/pullPolicy, replicas и resources из `values.yaml`, `PORT` из `proxyService.service.targetPort`, envFrom `cinemaabyss-config`, probes `/health`) и Service (`port: 80` -> `targetPort: 8000`);
- [events-service.yaml](src/kubernetes/helm/templates/services/events-service.yaml) — Deployment (аналогично, probes `/api/events/health`, `KAFKA_BROKERS` из configmap) и Service (8082);
- в [templates/configmap.yaml](src/kubernetes/helm/templates/configmap.yaml) исправлен `MOVIES_SERVICE_URL` (`http://movies:...` -> `http://movies-service:...` — сервис называется movies-service) и добавлены `EVENTS_SERVICE_URL`, `KAFKA_BROKERS`.

![Helm deploy и вывод /api/movies (docs/screenshots/helm-list-pods-curl.png)

# Задание 5
Компания планирует активно развиваться и для повышения надежности, безопасности, реализации сетевых паттернов типа Circuit Breaker и канареечного деплоя вам как архитектору необходимо развернуть istio и настроить circuit breaker для monolith и movies сервисов.

```bash

helm repo add istio https://istio-release.storage.googleapis.com/charts
helm repo update

helm install istio-base istio/base -n istio-system --set defaultRevision=default --create-namespace
helm install istio-ingressgateway istio/gateway -n istio-system
helm install istiod istio/istiod -n istio-system --wait

helm install cinemaabyss .\src\kubernetes\helm --namespace cinemaabyss --create-namespace

kubectl label namespace cinemaabyss istio-injection=enabled --overwrite

kubectl get namespace -L istio-injection

kubectl apply -f .\src\kubernetes\circuit-breaker-config.yaml -n cinemaabyss

```

Тестирование

# fortio
```bash
kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.25/samples/httpbin/sample-client/fortio-deploy.yaml -n cinemaabyss
```

# Get the fortio pod name
```bash
FORTIO_POD=$(kubectl get pod -n cinemaabyss | grep fortio | awk '{print $1}')

kubectl exec -n cinemaabyss $FORTIO_POD -c fortio -- fortio load -c 50 -qps 0 -n 500 -loglevel Warning http://movies-service:8081/api/movies
```
Например,

```bash
kubectl exec -n cinemaabyss fortio-deploy-b6757cbbb-7c9qg  -c fortio -- fortio load -c 50 -qps 0 -n 500 -loglevel Warning http://movies-service:8081/api/movies
```

Вывод будет типа такого

```bash
IP addresses distribution:
10.106.113.46:8081: 421
Code 200 : 79 (15.8 %)
Code 500 : 22 (4.4 %)
Code 503 : 399 (79.8 %)
```
Можно еще проверить статистику

```bash
kubectl exec -n cinemaabyss fortio-deploy-b6757cbbb-7c9qg -c istio-proxy -- pilot-agent request GET stats | grep movies-service | grep pending
```

И там смотрим 

```bash
cluster.outbound|8081||movies-service.cinemaabyss.svc.cluster.local;.upstream_rq_pending_total: 311 - столько раз срабатывал circuit breaker
You can see 21 for the upstream_rq_pending_overflow value which means 21 calls so far have been flagged for circuit breaking.
```

Приложите скриншот работы circuit breaker'а

**Решение.** Создан [src/kubernetes/circuit-breaker-config.yaml](src/kubernetes/circuit-breaker-config.yaml) — две `DestinationRule` (Istio) для хостов `monolith` и `movies-service`:

- `connectionPool.tcp.maxConnections: 1`, `http.http1MaxPendingRequests: 1`, `maxRequestsPerConnection: 1` — при конкурентной нагрузке (fortio `-c 50`) лишние запросы мгновенно отбрасываются Envoy с кодом 503 — это и есть срабатывание circuit breaker;
- `outlierDetection` (`consecutive5xxErrors: 1`, `interval: 1s`, `baseEjectionTime: 3m`, `maxEjectionPercent: 100`) — инстансы, отвечающие 5xx, временно исключаются из балансировки.

Порядок применения — по командам выше:
 установка Istio (base, ingressgateway, istiod),
 установка чарта,
 включение sidecar-injection для namespace (`istio-injection=enabled`, поды нужно пересоздать после включения),
 затем `kubectl apply -f src/kubernetes/circuit-breaker-config.yaml -n cinemaabyss`.
 Проверка — fortio load `-c 50 -qps 0 -n 500`
  на `http://movies-service:8081/api/movies`: значительная доля ответов 503,
  счетчики `upstream_rq_pending_total` / `upstream_rq_pending_overflow`
  в статистике istio-proxy показывают срабатывания circuit breaker.

![Fortio статистика](docs/screenshots/istio-fortio.png)
![Circuit breaker stats](docs/screenshots/istio-cb-stats.png)

Удаляем все
```bash
istioctl uninstall --purge
kubectl delete namespace istio-system
kubectl delete all --all -n cinemaabyss
kubectl delete namespace cinemaabyss
```
