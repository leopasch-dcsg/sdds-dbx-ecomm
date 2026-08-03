from enum import Enum


class WebstoreCode(Enum):
    G3 = ("G3", "Going Going Gone")
    GolfGalaxy = ("GolfGalaxy", "Golf Galaxy")
    DicksSportingGoods = ("DicksSportingGoods", "DSG - Insourced")
    Moosejaw = ("Moosejaw", "Moosejaw")
    PublicLands = ("PublicLands", "Public Lands")
    OTHER = (None, None)

    def __init__(self, code: str, desc: str):
        self.code = code
        self.desc = desc
