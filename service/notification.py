from common.service import run_service

from common.topology import RK_EMAIL_SENT, Q_NOTIFICATION

SERVICE_NAME = "notification-service"   

def process(event):
    return event.payload.model_copy(update={"status": "NOTIFIED"})

run_service(SERVICE_NAME, Q_NOTIFICATION, RK_EMAIL_SENT, process)