from enum import Enum


class ChannelCode(Enum):
    NOT_APPLICABLE = (-999, "N/A", "Not Applicable")
    UNKNOWN = (-1, "UNK", "Unknown")
    BOPIS = (3, "Bopis", "Buy Online Pick Up In Store")
    SFS = (4, "SFS", "Ship From Store")
    DC = (5, "DC", "Distribution Center")
    VDC = (6, "VDC", "Vendor Direct")
    RDC = (7, "RDC", "Regional DC (backstock)")
    BOPL = (8, "BOPL", "Buy Online Pickup Later (Ship to Store)")
    MULTI = (12, "MULT", "Multiple Channels")

    def __init__(self, key: int, code: str, description: str):
        self.key = key
        self.code = code
        self.description = description
