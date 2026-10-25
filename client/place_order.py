from common.broker import connect, publish_event                          
from common.events import Customer, Event, Item, Order                    
from common.topology import EVENT_TYPES, RK_ORDER_REQUESTED 

SERVICE_NAME = "client-app"                                                      

connection, channel = connect() 

order = Order(
    order_id="ORD-001",
    customer=Customer(name="Ana", email="ana@example.com"),
    items=[
        Item(sku="SKU-1", name="Mouse", unit_price=549.0, quantity=2),
    ],
    total=1098.0,
)

event = Event(
    event_type=EVENT_TYPES[RK_ORDER_REQUESTED],                          
    producer=SERVICE_NAME,
    payload=order,
)

publish_event(channel, SERVICE_NAME, RK_ORDER_REQUESTED, event) 

connection.close()