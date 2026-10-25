from common.broker import connect, publish_event
from common.events import Event, deserialize
from common.logger import log_event
from common.topology import EVENT_TYPES


def run_service(service, queue, output_routing_key, process):
    """
    service:            name service
    queue:              queue consumer
    output_routing_key: RK fo the event
    process(event):     updates order
    """
    connection, channel = connect()

    def on_message(ch, method, properties, body):
        event = deserialize(body)
        log_event(service, "consumed", event, method.routing_key)

        order = process(event)

        new_event = Event(
            event_type=EVENT_TYPES[output_routing_key],
            producer=service,
            payload=order,
        )
        publish_event(ch, service, output_routing_key, new_event)


    channel.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=True)
    print(f"[{service}] escuchando {queue}...")
    channel.start_consuming()