import os
import time
from datetime import datetime, timezone

LOG_DIR = "/app/logs"
LOG_FILE = os.path.join(LOG_DIR, "events.log")
LOCK_DIR = LOG_FILE + ".lock"     # carpeta que funciona como lock entre contenedores

LOCK_TIMEOUT = 5      # segundos maximos esperando el lock
LOCK_RETRY = 0.005    # segundos entre intentos (5 ms)


def _acquire_lock():
    """Toma el lock creando una carpeta. Crear una carpeta es atomico:
    si dos contenedores lo intentan al mismo tiempo, solo uno lo logra."""
    start = time.monotonic()
    while True:
        try:
            os.mkdir(LOCK_DIR)
            return
        except FileExistsError:
            # Otro contenedor esta escribiendo. Si lleva demasiado tiempo,
            # probablemente se cayo sin soltar el lock: lo liberamos.
            if time.monotonic() - start > LOCK_TIMEOUT:
                _release_lock()
                start = time.monotonic()
            time.sleep(LOCK_RETRY)


def _release_lock():
    try:
        os.rmdir(LOCK_DIR)
    except FileNotFoundError:
        pass


def _append_line(text):
    """Agrega una linea al final del archivo, turnandose con los demas contenedores."""
    os.makedirs(LOG_DIR, exist_ok=True)
    _acquire_lock()
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(text)
            f.flush()                # del buffer de Python al sistema operativo
            os.fsync(f.fileno())     # del sistema operativo al disco
    finally:
        _release_lock()              # soltar siempre, aunque algo falle


def log_event(service, action, event, routing_key):
    """action: 'produced' o 'consumed'."""
    timestamp = datetime.now(timezone.utc).isoformat()

    line = " | ".join([
        timestamp,
        service,
        action,
        event.event_type,
        routing_key,
        event.payload.order_id,
        str(event.event_id),
    ])

    print(line, flush=True)
    _append_line(line + "\n")
