INT8 = 1
INT16 = 2
INT32 = 4
INT64 = 8
NULL = int.from_bytes(b"\xff")
COMPACT_STRING_LEN = INT8
NULLABLE_STRING_LEN = INT16


# split(n)
# b1,b2 = b[:n],b[:n]
# digest(type):
# b1,b2 = b[:type.n],b[:type.n]
# return type.from_bytes(b1), b2


def str_from_bytes(b: bytes) -> str:
    return b.decode()


def sum_bytes(data: list[bytes]) -> bytes:
    acc = data[0]
    for b in data[1:]:
        acc = acc + b

    return acc
