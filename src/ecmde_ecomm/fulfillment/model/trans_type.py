from enum import Enum


class TransType(Enum):
    # There is indeed a typo in the raw data as "Not Application" instead of "Not Applicable"
    # We will maintain the typo for consistency with the data.
    NotApplicable = (-999, -999, "N/A", "Not Application")
    Unknown = (-1, -1, "UNK", "Unknown")
    Cancelled = (999, 5, "CA", "Cancelled")
    SaveOfferSent = (111, 17, "SS", "Save the Sale Offer Sent")
    SaveOfferAccepted = (112, 17, "SS", "Save the Sale Offer Accepted")
    SaveOfferRejected = (113, 17, "SS", "Save the Sale Offer Rejected")
    SaveOfferExpired = (114, 17, "SS", "Save the Sale Offer Expired")
    Allocated = (200, 3, "AC", "Item status allocated")
    Declined = (790, 3, "DC", "Item status declined")
    Fulfilled = (800, 6, "FF", "Fulfilled")
    Returned = (8, 8, "RT", "Returned")
    PriceAdjustment = (11, 11, "PA", "Price Adjustment")

    def __init__(self, item_status: int, key: int, code: str, description: str):
        self.item_status = item_status
        self.key = key
        self.code = code
        self.description = description
