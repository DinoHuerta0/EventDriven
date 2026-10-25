import pika
from common.events import Order, Item, Customer, Event, serialize
from common.topology import declare_topology, EXCHANGE_NAME, RK_ORDER_PLACED

connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
channel = connection.channel()
declare_topology(channel)

order = Order(
    order_id="ORD-001",
    customer=Customer(name="Ana", email="ana@example.com"),
    items=[
        Item(sku="SKU-1", name="Mouse", unit_price=549.0, quantity=2),
    ],
    total=1098.0,
)

event = Event(event_type="OrderPlaced", producer="test-producer", payload=order)

channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=RK_ORDER_PLACED, body=serialize(event))
print(f"Enviado | {event.event_type} | event_id={event.event_id}")

connection.close()