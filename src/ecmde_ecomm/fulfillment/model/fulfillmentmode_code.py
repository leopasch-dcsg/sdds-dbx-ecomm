from enum import Enum


class FulfillmentModeCode(Enum):
    GROUND = (266, "GROUND", "Ground")
    UNKNOWN = (-1, "UNK", "Unknown")
    BOPIS = (265, "BOPIS", "Buy Online Pick Up In Store")
    STANDARD = (266, "STANDARD", "Ship From Store")
    PO_BOX = (285, "PO_BOX", "PO_BOX")
    EXPEDITED = (270, "EXPEDITED", "EXPEDITED")
    EXPRESS = (271, "EXPRESS", "EXPRESS")
    BOPL = (292, "BOPL", "Buy Online Pickup Later (Ship to Store)")
    SAMEDAY = (302, "SAMEDAY", "SAMEDAY")
    APO = (280, "APO", "APO")
    CURBSIDE = (281, "CURBSIDE", "Curbside Pickup")
    ROOM_OF_CHOICE = (282, "ROC", "Room of Choice")
    THRESHOLD = (283, "THRESHLD", "Threshold Delivery")
    DELIVERY_ASSEMBLY = (284, "ASSEMBLE", "Assembly")
    DDSD = (298, "DDSD", "DoorDash - AX")

    def __init__(self, key: int, code: str, description: str):
        self.key = key
        self.code = code
        self.description = description
