from enum import Enum


class ChainCode(Enum):
    G3 = (8, "G3", "G3")
    GolfGalaxy = (2, "GolfGalaxy", "Golf Galaxy")
    DicksSportingGoods = (6, "DicksSportingGoods", "Dick's Sporting Goods")
    Moosejaw = (9, "Moosejaw", "Moosejaw")
    PublicLands = (7, "PublicLands", "Public Lands")
    UNK = (-1, "UNK", "Unknown or Other")

    def __init__(self, key: int, code: str, description: str):
        self.key = key
        self.code = code
        self.description = description
