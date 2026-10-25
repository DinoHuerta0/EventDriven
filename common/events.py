from uuid import UUID, uuid4
from datetime import datetime, timezone

from pydantic import BaseModel, Field

class Customer(BaseModel):
    name: str
    email: str
        
class Item(BaseModel):
    sku: str
    name: str
    unit_price: float
    quantity: int
    
class Order(BaseModel):
    order_id: str
    customer: Customer
    items: list[Item]
    total: float
    currency: str = "MXN"
    status: str = "REQUESTED" 
    
def utc_now():
    return datetime.now(timezone.utc)

class Event(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    timestamp: datetime = Field(default_factory=utc_now)
    producer: str
    payload:  Order
        
def serialize(event):
    return event.model_dump_json().encode("utf-8")


def deserialize(body):
    return Event.model_validate_json(body)
