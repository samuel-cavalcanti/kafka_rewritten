INT8 = 1
INT16 = 2
INT32 = 4
NULL = int.from_bytes(b"\xff")
COMPACT_STRING_LEN = INT8
NULLABLE_STRING_LEN = INT16



def sum_bytes(data: list[bytes]) -> bytes:
    acc = data[0]
    for b in data[1:]:
        acc = acc + b

    return acc
