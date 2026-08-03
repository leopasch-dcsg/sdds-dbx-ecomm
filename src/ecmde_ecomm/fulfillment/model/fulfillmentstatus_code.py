from enum import Enum


class FulfillmentStatus(Enum):
    FULFILLED = (800, "F", "Fulfilled")
    CANCELLED = (999, "X", "Cancelled")
    MANIFESTED = (710, "M", "Manifested")
    DECLINED = (790, "X", "Declined")
    RECEIVED = (810, "X", "Received")
    STSACCEPTED = (112, "X", "STS Accepted")
    STSREJECTED = (113, "X", "STS Rejected")
    STSEXPIRED = (114, "X", "STS Expired") 
    OTHER = (None, "A", "Acknowledged / All Other")
    PICKED = (None, "P", "Picked")
    SHIPPED = (None, "S", "Shipped")

    def __init__(self, status_id: int, code: str, description: str):
        self.status_id = status_id
        self.code = code
        self.description = description
