from common.service import run_service
from common.topology import Q_PAYMENT, RK_PAYMENT_APPLIED

SERVICE_NAME = "payment-service"


def process(event):
    return event.payload.model_copy(update={"status": "PAID"})

run_service(SERVICE_NAME, Q_PAYMENT, RK_PAYMENT_APPLIED, process)