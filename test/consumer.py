import pika
from common.events import deserialize
from common.topology import declare_topology

connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
channel = connection.channel()
declare_topology(channel)

def on_message(ch, method, properties, body):
    event = deserialize(body)
    print(f"Recibido | {event.event_type} | de {event.producer} | orden {event.payload.order_id} | total {event.payload.total}")
    
channel.basic_consume(queue="payment.queue", on_message_callback=on_message, auto_ack=True)
print("Esperando mensaje... (Ctrl + C para abrir)")
channel.start_consuming()