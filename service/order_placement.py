from common.service import run_service
from common.topology import RK_ORDER_PLACED, Q_ORDER_PLACEMENT

SERVICE_NAME = "order-placement-service"   

def process(event):
    return event.payload.model_copy(update={"status": "PLACED"})


run_service(SERVICE_NAME, Q_ORDER_PLACEMENT,  RK_ORDER_PLACED, process)



