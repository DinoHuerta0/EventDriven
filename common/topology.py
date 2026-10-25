EXCHANGE_NAME = "order.events"
EXCHANGE_TYPE = "topic"

RK_ORDER_REQUESTED = "order.requested"
RK_ORDER_PLACED = "order.placed"
RK_PAYMENT_APPLIED = "payment.applied"
RK_ORDER_FULFILLED = "order.fulfilled"
RK_EMAIL_SENT = "notification.email.sent"

Q_ORDER_PLACEMENT = "order_placement.queue"
Q_PAYMENT = "payment.queue"
Q_ORDER_FULFILLMENT = "order_fulfillment.queue"
Q_NOTIFICATION = "notification.queue"

QUEUES = {
    Q_ORDER_PLACEMENT: [RK_ORDER_REQUESTED],
    Q_PAYMENT: [RK_ORDER_PLACED],
    Q_ORDER_FULFILLMENT: [RK_PAYMENT_APPLIED],
    Q_NOTIFICATION: [RK_ORDER_FULFILLED],
}


def declare_topology(channel):
    """Declare exchange, queues and bindings."""
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)
    for queue, routing_keys in QUEUES.items():
        channel.queue_declare(queue=queue)
        for rk in routing_keys:
            channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue, routing_key=rk)