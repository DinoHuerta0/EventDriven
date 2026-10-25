import os
from datetime import datetime, timezone

LOG_DIR = "/app/logs"
LOG_FILE = os.path.join(LOG_DIR, "events.log")


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

    os.makedirs(LOG_DIR, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")