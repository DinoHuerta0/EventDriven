# Event Driven Architecture: flujo de órdenes con RabbitMQ (topics) y Docker

> Arquitectura de Software — ITESO
> Autor(es): Victor Sebastian Sanchez Silva
> Fecha: 2026-10-25

Simula el flujo de una orden de compra con una **arquitectura orientada a eventos**. Cuatro servicios independientes se comunican solo a través de eventos publicados en un **topic exchange de RabbitMQ**. Cada servicio, el broker y el cliente corren en contenedores separados con Docker Compose, los eventos se validan con **Pydantic**, y cada paso del flujo queda registrado en un log que se genera al ejecutar la aplicación.

---

## Tabla de contenidos
- [Objetivo](#objetivo)
- [Arquitectura](#arquitectura)
- [Tecnologías](#tecnologías)
- [Requisitos previos](#requisitos-previos)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Configuración](#configuración)
- [Despliegue / Ejecución](#despliegue--ejecución)
- [Verificación](#verificación)
- [Resultados](#resultados)
- [Limpieza de recursos](#limpieza-de-recursos)
- [Problemas encontrados y soluciones](#problemas-encontrados-y-soluciones)
- [Referencias](#referencias)

---

## Objetivo
Simular el flujo de una orden, desde que se coloca hasta que se notifica al cliente, usando servicios desacoplados que solo se comunican mediante eventos.

- Implementar 4 servicios: **Order Placement**, **Payment**, **Order Fulfillment** y **Notification**.
- Usar **RabbitMQ con un topic exchange** como broker de eventos.
- Correr cada servicio y el broker en un **contenedor separado** (Dockerfile + docker-compose.yml).
- Definir una **estructura de evento común**: `event_id`, `event_type`, `timestamp`, `producer` y `payload`.
- Disparar el flujo desde consola con `place_order.py` (initiating event).
- Generar **evidencia dinámica** del flujo: quién produjo y quién consumió cada evento.

> No se implementan cobros reales ni envío de correos: el pago y el email se **simulan** generando sus eventos.

## Arquitectura
El flujo es una **coreografía**: no hay un orquestador central. Cada servicio escucha su cola, procesa el evento y publica el siguiente. Ningún servicio conoce a los demás; solo conocen los eventos.

```
                               RabbitMQ
                   ┌──────── exchange: order.events (topic) ──────────┐
                   │                                                    │
 place_order.py ──►│ order.requested ──► [order_placement.queue] ───────┼──► Order Placement Service
  (cliente)        │                                                    │            │
                   │ order.placed    ──► [payment.queue] ───────────────┼──► Payment Service
                   │                                                    │            │
                   │ payment.applied ──► [order_fulfillment.queue] ─────┼──► Order Fulfillment Service
                   │                                                    │            │
                   │ order.fulfilled ──► [notification.queue] ──────────┼──► Notification Service
                   │                                                    │            │
                   │ notification.email.sent  (fin del flujo)  ◄────────┼────────────┘
                   └────────────────────────────────────────────────────┘
```

### Servicios y eventos

| Servicio | Consume | Produce | Estado de la orden |
|---|---|---|---|
| Cliente (`place_order.py`) | — | `OrderRequested` | `REQUESTED` |
| Order Placement Service | `OrderRequested` | `OrderPlaced` | `PLACED` |
| Payment Service | `OrderPlaced` | `PaymentApplied` | `PAID` |
| Order Fulfillment Service | `PaymentApplied` | `OrderFulfilled` | `FULFILLED` |
| Notification Service | `OrderFulfilled` | `EmailSent` | `NOTIFIED` |

Cada servicio actualiza el `status` de la orden y lo envía en el siguiente evento (*event-carried state transfer*).

### Topics (routing keys) y colas

| Evento | Routing key | Cola que lo recibe |
|---|---|---|
| `OrderRequested` | `order.requested` | `order_placement.queue` |
| `OrderPlaced` | `order.placed` | `payment.queue` |
| `PaymentApplied` | `payment.applied` | `order_fulfillment.queue` |
| `OrderFulfilled` | `order.fulfilled` | `notification.queue` |
| `EmailSent` | `notification.email.sent` | — (último evento del flujo) |

**¿Por qué un topic exchange?** RabbitMQ no usa topics por defecto: si se publica sin exchange, se usa el *default exchange*, que rutea por nombre de cola. Aquí se declara explícitamente un exchange de tipo `topic` y cada cola se liga (*binding*) con una routing key. Los patrones admiten comodines (`*` = exactamente una palabra, `#` = cero o más palabras), así que se pueden agregar nuevos suscriptores sin modificar a los productores. Por ejemplo, un servicio de auditoría ligado a `#` recibiría todos los eventos.

`EmailSent` no tiene cola porque nadie lo consume: el Notification Service solo anuncia que terminó. RabbitMQ descarta los mensajes que no coinciden con ninguna cola; si en el futuro se agrega un suscriptor, recibirá los eventos desde ese momento.

Toda la topología (exchange, colas, bindings y la relación routing key → `event_type`) está definida una sola vez en `common/topology.py`. Los servicios usan constantes en lugar de textos, así un nombre mal escrito falla de inmediato con `NameError` en vez de perder mensajes en silencio.

### Estructura de un evento

Los eventos se definen como modelos de **Pydantic** en `common/events.py`. Se validan al crearse (productor) y al recibirse (consumidor); `event_id` y `timestamp` se generan automáticamente.

```json
{
  "event_id": "44b07f5a-692f-4505-b4db-a73002d6bc19",
  "event_type": "OrderPlaced",
  "timestamp": "2026-10-25T23:16:03.592530Z",
  "producer": "order-placement-service",
  "payload": {
    "order_id": "ORD-D78EEABB",
    "customer": { "name": "Ana", "email": "ana@example.com" },
    "items": [
      { "sku": "SKU-1", "name": "Mouse", "unit_price": 549.0, "quantity": 2 }
    ],
    "total": 1098.0,
    "currency": "MXN",
    "status": "PLACED"
  }
}
```

| Campo | Descripción |
|---|---|
| `event_id` | Identificador único del evento (UUID v4) |
| `event_type` | Nombre del evento |
| `timestamp` | Momento en que se produjo (ISO-8601, UTC) |
| `producer` | Servicio que lo emitió |
| `payload` | Datos de la orden (`Order`, con `Customer` e `Item` anidados) |

La estructura está inspirada en el estándar **CloudEvents** (`id`, `type`, `time`, `source`, `data`).

### Código compartido (`common/`)

| Módulo | Responsabilidad |
|---|---|
| `topology.py` | Exchange, routing keys, colas, bindings y `declare_topology()` |
| `events.py` | Modelos Pydantic y serialización a JSON |
| `broker.py` | `connect()` y `publish_event()` (publica y deja evidencia en un solo paso) |
| `service.py` | `run_service()`: el ciclo común consumir → procesar → producir |
| `logger.py` | Evidencia en consola y en `logs/events.log` |

Gracias a `run_service()`, cada servicio solo define lo que lo hace único: su nombre, su cola, la routing key que produce y su función `process()`. Por ejemplo, `service/payment.py`:

```python
def process(event):
    return event.payload.model_copy(update={"status": "PAID"})

run_service(SERVICE_NAME, Q_PAYMENT, RK_PAYMENT_APPLIED, process)
```

## Tecnologías
| Herramienta | Versión | Uso |
|-------------|---------|-----|
| Docker | — | Un contenedor por servicio, broker y cliente |
| Docker Compose | v2 | Orquestación, healthcheck, red y volumen de logs |
| Python | 3.12 (`python:3.12-slim`) | Lenguaje de los servicios y del cliente |
| RabbitMQ | 3.13-management | Broker de eventos (topic exchange) y panel web |
| pika | 1.3.2 | Cliente de RabbitMQ para Python (AMQP 0-9-1) |
| Pydantic | 2.9.2 | Definición y validación de la estructura de los eventos |

Se usa la imagen `slim` porque las dependencias no necesitan compiladores, lo que reduce la imagen de ~1 GB a ~130 MB.

## Requisitos previos
- **Docker Desktop** (Windows/Mac) o **Docker Engine con Compose v2** (Linux).
  ```bash
  docker --version
  ```
  ```bash
  docker compose version
  ```
- Puertos **5672** (AMQP) y **15672** (panel de RabbitMQ) libres.
- No se necesita Python instalado localmente: todo corre dentro de los contenedores.

## Estructura del proyecto
```
EventDriven/
├── client/
│   └── place_order.py         # Cliente: pone la orden y emite OrderRequested (initiating event)
├── common/
│   ├── __init__.py
│   ├── broker.py              # Conexión a RabbitMQ y publicación con evidencia
│   ├── events.py              # Modelos Pydantic del evento y la orden
│   ├── logger.py              # Evidencia: consola y logs/events.log (con lock entre contenedores)
│   ├── service.py             # Ciclo común de un servicio: consumir → procesar → producir
│   └── topology.py            # Exchange topic, routing keys, colas y bindings
├── docker/
│   ├── docker-compose.yml     # Broker, 4 servicios y cliente (app)
│   ├── Dockerfile             # Imagen única que usan todos los servicios y el cliente
│   └── requirements.txt       # Dependencias de Python (pika, pydantic)
├── logs/                      # Se llena al ejecutar (events.log)
│   └── .gitkeep
├── service/
│   ├── __init__.py
│   ├── order_placement.py     # OrderRequested → OrderPlaced
│   ├── payment.py             # OrderPlaced → PaymentApplied
│   ├── order_fulfillment.py   # PaymentApplied → OrderFulfilled
│   └── notification.py        # OrderFulfilled → EmailSent
├── .gitignore
└── README.md
```

Los 5 contenedores de Python usan **la misma imagen** (`event-driven:latest`); cada uno arranca con un `command` distinto en `docker-compose.yml`. El Dockerfile copia el código dentro de la imagen y deja `client/place_order.py` directamente en `/app`, por eso el comando de ejecución es `python place_order.py`.

## Configuración
No hace falta configurar nada para correrlo. Puntos relevantes del `docker-compose.yml`:

| Elemento | Para qué |
|---|---|
| `context: ..` / `dockerfile: docker/Dockerfile` | El compose vive en `docker/`, pero el contexto de build es la raíz para poder copiar `common/`, `service/` y `client/` |
| `image: event-driven:latest` en los 5 servicios | Todos comparten una sola imagen; al reconstruir los servicios también se actualiza la del cliente |
| `healthcheck` con `check_port_connectivity` | RabbitMQ solo se marca `healthy` cuando el puerto AMQP ya acepta conexiones |
| `depends_on: condition: service_healthy` | Los servicios y el cliente esperan a que RabbitMQ esté listo |
| `profiles: ["cli"]` en `app` | El cliente no arranca con `docker compose up`; solo con `docker compose run` |
| `../logs:/app/logs` | La evidencia se escribe en la carpeta `logs/` local (sin `:ro`, porque se escribe) |

## Despliegue / Ejecución
Todos los comandos de Docker Compose se ejecutan **desde la carpeta `docker/`**, donde está `docker-compose.yml`.

**0. Entrar a la carpeta `docker/`**
```bash
cd docker
```

**1. Construir la imagen y levantar el broker y los 4 servicios**
```bash
docker compose up -d --build
```

**2. Verificar que estén corriendo**
```bash
docker compose ps
```
Debe aparecer `rabbitmq` como `healthy` y los 4 servicios como `running`. Los servicios tardan unos segundos en arrancar porque esperan el healthcheck de RabbitMQ. El contenedor `app` no aparece porque solo se crea al poner una orden.

**3. Poner una orden (initiating event)**
```bash
docker compose run --rm app python place_order.py
```

Cada ejecución genera una orden con un `order_id` único (por ejemplo `ORD-D78EEABB`).

## Verificación

**1. Log de eventos** (se crea al ejecutar, en la carpeta `logs/` de la raíz)
```bash
cat ../logs/events.log
```

Formato de cada línea:
```
timestamp | servicio | acción | event_type | routing_key | order_id | event_id
```

Resultado esperado para **una orden**: 9 líneas.
```
2026-10-25T23:16:03.575953+00:00 | client-app | produced | OrderRequested | order.requested | ORD-D78EEABB | 32aee11f-...
2026-10-25T23:16:03.577149+00:00 | order-placement-service | consumed | OrderRequested | order.requested | ORD-D78EEABB | 32aee11f-...
2026-10-25T23:16:03.592530+00:00 | order-placement-service | produced | OrderPlaced | order.placed | ORD-D78EEABB | 44b07f5a-...
2026-10-25T23:16:03.593686+00:00 | payment-service | consumed | OrderPlaced | order.placed | ORD-D78EEABB | 44b07f5a-...
2026-10-25T23:16:03.609832+00:00 | payment-service | produced | PaymentApplied | payment.applied | ORD-D78EEABB | 2ce70945-...
2026-10-25T23:16:03.611235+00:00 | order-fulfillment-service | consumed | PaymentApplied | payment.applied | ORD-D78EEABB | 2ce70945-...
2026-10-25T23:16:03.627249+00:00 | order-fulfillment-service | produced | OrderFulfilled | order.fulfilled | ORD-D78EEABB | 61506e83-...
2026-10-25T23:16:03.628620+00:00 | notification-service | consumed | OrderFulfilled | order.fulfilled | ORD-D78EEABB | 61506e83-...
2026-10-25T23:16:03.642551+00:00 | notification-service | produced | EmailSent | notification.email.sent | ORD-D78EEABB | fe775e88-...
```

> Las 9 líneas **no son 9 servicios**, son 9 acciones: el cliente produce 1 evento y cada uno de los 4 servicios aparece dos veces (consume un evento y produce el siguiente). El `event_id` de cada `produced` coincide con el del `consumed` de la línea siguiente: es el mismo evento viajando de un servicio a otro.

Para contar las líneas (deben ser 9 por orden):
```bash
wc -l ../logs/events.log
```

Para ver solo una orden cuando hay varias:
```bash
grep ORD-D78EEABB ../logs/events.log
```

**2. Logs en vivo de los contenedores** (sal con `Ctrl + C`)
```bash
docker compose logs -f
```

**3. Panel de RabbitMQ**

Abrir [http://localhost:15672](http://localhost:15672) (usuario `guest`, contraseña `guest`):
- **Exchanges → `order.events`**: tipo `topic` y sus 4 bindings.
- **Queues**: las 4 colas, cada una con 1 consumidor.

Para ver el desacoplamiento, detén un servicio, pon una orden y revisa su cola: el mensaje queda esperando en *Ready* y se procesa en cuanto el servicio vuelve a arrancar.
```bash
docker compose stop payment
```
```bash
docker compose run --rm app python place_order.py
```
```bash
docker compose start payment
```

## Resultados
<!-- Agrega aquí tus capturas en docs/ y tus conclusiones -->

![Servicios corriendo con docker compose ps](docs/compose_ps.png)

![Ejecución de place_order.py](docs/place_order.png)

![Log de eventos](docs/events_log.png)

![Exchange topic en el panel de RabbitMQ](docs/rabbitmq_exchange.png)

![Colas en el panel de RabbitMQ](docs/rabbitmq_queues.png)

- Con un solo comando (`place_order.py`) se dispara el flujo completo y los 4 servicios reaccionan en cadena sin conocerse entre sí.
- El topic exchange permite agregar nuevos consumidores sin modificar ningún servicio existente.
- Pydantic define un contrato explícito para los eventos: un evento mal formado se rechaza en el momento con un error claro.
- Centralizar la topología y el ciclo de los servicios en `common/` redujo cada servicio a unas pocas líneas y eliminó errores de nombres mal escritos.

## Limpieza de recursos
Desde la carpeta `docker/`, detener y eliminar los contenedores y la red:
```bash
docker compose down --remove-orphans
```

Para borrar también la imagen construida:
```bash
docker compose down --remove-orphans --rmi local
```

Los logs se acumulan entre ejecuciones (el archivo se abre en modo *append*). Para empezar de cero:
```bash
rm -f ../logs/events.log
```

## Problemas encontrados y soluciones
Problemas reales que aparecieron durante el desarrollo y cómo se resolvieron.

| Problema | Causa | Solución |
|----------|-------|----------|
| `service "app" refers to undefined volume \EventDriven\pruebas` | La ruta del volumen usaba `\` y no empezaba con `.` ni `/`; Compose la interpretó como un *named volume* | Rutas relativas al compose con `/`: `../logs:/app/logs` |
| `COPY requirements.txt ./` → `"/requirements.txt": not found` | El origen del `COPY` es relativo al **contexto** (la raíz), no a la carpeta del Dockerfile | `COPY docker/requirements.txt ./` |
| `ModuleNotFoundError: No module named 'common'` | El contenedor no tenía la carpeta `common/` o Python no buscaba en `/app` | Copiar `common/` a la imagen y definir `ENV PYTHONPATH=/app` en el Dockerfile |
| `ls: cannot access 'C:/Program Files/Git/app'` al correr comandos en el contenedor | Git Bash convierte automáticamente las rutas que empiezan con `/` a rutas de Windows | Usar `//app` o anteponer `MSYS_NO_PATHCONV=1` al comando |
| `pika.exceptions.AMQPConnectionError` al arrancar | `depends_on` solo espera a que el **contenedor** de RabbitMQ arranque, no a que acepte conexiones | Healthcheck en RabbitMQ y `depends_on: condition: service_healthy` |
| Los servicios quedaban en `Created` y nunca arrancaban | Se usó `condition: service_healthy` sin definir un `healthcheck` en RabbitMQ | Agregar el bloque `healthcheck` al servicio `rabbitmq` |
| Con healthcheck, los servicios seguían cayendo con `AMQPConnectionError` (`Exited (1)`) | `rabbitmq-diagnostics ping` responde cuando el nodo está vivo, pero el puerto 5672 tarda unos segundos más en abrirse | Cambiar el healthcheck a `rabbitmq-diagnostics -q check_port_connectivity` |
| El productor imprimía "Enviado" pero el mensaje nunca llegaba | Las constantes se escribieron entre comillas (`exchange="EXCHANGE_NAME"`), así que se publicaba en un exchange que no existe; `basic_publish` no espera confirmación del broker | Usar las constantes sin comillas (`exchange=EXCHANGE_NAME`) |
| Mensajes procesados con código viejo y líneas inconsistentes en el log | Quedaron contenedores duplicados de `docker compose run` sin cerrar; RabbitMQ reparte los mensajes entre todos los consumidores de una cola (*round-robin*) | `docker compose down --remove-orphans`. Con todos los servicios en el compose, `up` levanta exactamente una copia de cada uno |
| Faltaban líneas en `events.log` y aparecían fragmentos sueltos (por ejemplo `db68`) | Varios contenedores escribían el mismo archivo al mismo tiempo; en un volumen de Windows, una escritura pisaba a la otra. `fcntl.flock` no protege entre contenedores en ese volumen | Lock basado en crear una carpeta (`events.log.lock`), que es una operación atómica del sistema de archivos, más `fsync` antes de soltarlo (ver `common/logger.py`) |
| `python: can't open file '/app/place_order.py'` y el `order_id` seguía siendo `ORD-001` después de cambiar el código | `app` está en el profile `cli`, así que `docker compose up --build` no reconstruía su imagen y usaba una versión vieja | Usar `image: event-driven:latest` en los 5 servicios para que compartan una sola imagen |
| El log tiene órdenes de ejecuciones anteriores | `events.log` se abre en modo *append* y vive en un volumen | Borrar `../logs/events.log` antes de una prueba limpia |
| `unrecognized arguments` al copiar comandos con comentarios `#` | zsh (macOS) y cmd no tratan `#` como comentario en modo interactivo | Copiar solo el comando, sin comentarios en la misma línea |

## Referencias
- [RabbitMQ: Topics (tutorial con Python)](https://www.rabbitmq.com/tutorials/tutorial-five-python)
- [RabbitMQ: Exchanges](https://www.rabbitmq.com/docs/exchanges)
- [RabbitMQ: Networking y puertos](https://www.rabbitmq.com/docs/networking)
- [pika](https://pika.readthedocs.io/)
- [Pydantic](https://docs.pydantic.dev/)
- [CloudEvents](https://cloudevents.io/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Imagen oficial de RabbitMQ](https://hub.docker.com/_/rabbitmq)

---

**Notas**
- `docker-compose.yml` está en `docker/`: los comandos `docker compose ...` se corren desde esa carpeta, o desde la raíz agregando `-f docker/docker-compose.yml`.
- El código está dentro de la imagen; después de modificar cualquier archivo `.py` hay que reconstruir con `docker compose up -d --build`.
