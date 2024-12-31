from enum import Enum


class CustomEnum(Enum):
    @classmethod
    def values(cls):
        return [enum.value for enum in cls]

    @classmethod
    def choices(cls):
        return [(enum.value, enum.name) for enum in cls]
