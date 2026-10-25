import pika

from common.events import deserialize, serialize, Event
from common.topology import declare_topology, EXCHANGE_NAME, RK_EMAIL_SENT, Q_NOTIFICATION
from common.logger import log_event

SERVICE_NAME = "notification-service"   

connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
channel = connection.channel()
declare_topology(channel)


def on_message(ch, method, properties, body):
    event = deserialize(body)
    log_event(SERVICE_NAME, "consumed", event, method.routing_key)
    
    order = event.payload.model_copy(update={"status": "NOTIFIED"})

    new_event = Event(event_type="EmailSent", producer=SERVICE_NAME, payload=order)                      
    ch.basic_publish(exchange=EXCHANGE_NAME, routing_key=RK_EMAIL_SENT, body=serialize(new_event))    
    log_event(SERVICE_NAME, "produced", new_event, RK_EMAIL_SENT)   


channel.basic_consume(queue=Q_NOTIFICATION, on_message_callback=on_message, auto_ack=True)             
print(f"[{SERVICE_NAME}] escuchando...")
channel.start_consuming()