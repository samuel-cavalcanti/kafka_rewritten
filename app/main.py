from enum import Enum, auto
import socket  # noqa: F401
from dataclasses import dataclass


@dataclass
class HeaderRequest:
    msg_size: int
    api_key: int
    api_version: int
    correlation_id: int


@dataclass
class ApiVersion0:
    error_code: int
    api_keys: list[int]


@dataclass
class ApiVersion1_4:
    error_code: int
    api_keys: list[int]
    throttle_time_ms: int


INT16 = 2
INT32 = 4


class ErrorCode(Enum):
    UNKNOWN_SERVER_ERROR = -1
    NONE = 0
    OFFSET_OUT_OF_RANGE = 1
    CORRUPT_MESSAGE = 2
    UNSUPPORTED_VERSION = 35


def correlation_id_respnse(id: int, msg_size: int) -> bytes:
    # The correlation_id field is a 32-bit signed integer.
    # The message_size field is a 32-bit signed integer
    bytes_len = 4  # 32/4
    response_bytes = msg_size.to_bytes(bytes_len) + id.to_bytes(bytes_len)
    return response_bytes


def parse_request_bytes(data: bytes) -> HeaderRequest:
    msg_size = data[:4]
    api_key = data[4:6]
    api_version = data[6:8]
    correlation_id = data[8 : 8 + 4]

    return HeaderRequest(
        int.from_bytes(msg_size),
        int.from_bytes(api_key),
        int.from_bytes(api_version),
        int.from_bytes(correlation_id),
    )


def api_version_response(header: HeaderRequest) -> bytes:
    def api_keys(api_key: int) -> bytes:
        return api_key.to_bytes(INT16) + (0).to_bytes(INT16) + (4).to_bytes(INT16)

    def header_bytes(header: HeaderRequest) -> bytes:
        return header.correlation_id.to_bytes(INT32)
        return header.msg_size.to_bytes(INT32) + header.correlation_id.to_bytes(INT32)

    print("api keys", api_keys(header.api_key))

    match header.api_version:
        case 0:
            return (
                header_bytes(header)
                + ErrorCode.NONE.value.to_bytes(INT16)
                + api_keys(header.api_key)
            )
        case 1 | 2 | 3 | 4:
            throttle_time_ms = 0
            return (
                header_bytes(header)
                + ErrorCode.NONE.value.to_bytes(INT16)
                + api_keys(header.api_key)
                + throttle_time_ms.to_bytes(INT32)
                + (0).to_bytes(INT32)
            )
        case _:
            return (
                header.msg_size.to_bytes(INT32)
                + header.correlation_id.to_bytes(INT32)
                + ErrorCode.UNSUPPORTED_VERSION.value.to_bytes(INT16)
            )


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client_socket, _ = server.accept()  # wait for client

    data = client_socket.recv(1024)

    print("input", data, len(data))
    header = parse_request_bytes(data)

    print("header", header)

    response_bytes = api_version_response(header)
    msg_size = len(response_bytes)
    response_bytes = msg_size.to_bytes(INT32) + response_bytes
    print("output", response_bytes, len(response_bytes))
    client_socket.send(response_bytes)


if __name__ == "__main__":
    main()
