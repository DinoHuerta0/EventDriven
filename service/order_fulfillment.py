from common.service import run_service
from common.topology import RK_ORDER_FULFILLED, Q_ORDER_FULFILLMENT


SERVICE_NAME = "order-fulfillment-service"   

def process(event):
    return event.payload.model_copy(update={"status": "FULFILLED"})

run_service(SERVICE_NAME, Q_ORDER_FULFILLMENT, RK_ORDER_FULFILLED, process)