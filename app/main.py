from enum import Enum
import socket  # noqa: F401
from dataclasses import dataclass
import threading


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


@dataclass
class ApiVersionsRequest:
    client_software_name: str
    client_software_version: str


INT8 = 1
INT16 = 2
INT32 = 4


class ErrorCode(Enum):
    UNKNOWN_SERVER_ERROR = -1
    NONE = 0
    OFFSET_OUT_OF_RANGE = 1
    CORRUPT_MESSAGE = 2
    UNSUPPORTED_VERSION = 35


class ApiKeys(Enum):
    ApiVersions = 18


def correlation_id_respnse(id: int, msg_size: int) -> bytes:
    # The correlation_id field is a 32-bit signed integer.
    # The message_size field is a 32-bit signed integer
    bytes_len = 4  # 32/4
    response_bytes = msg_size.to_bytes(bytes_len) + id.to_bytes(bytes_len)
    return response_bytes


def parse_request_header_bytes(data: bytes) -> HeaderRequest:
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


def parse_api_version_request(data: bytes) -> ApiVersionsRequest:
    COMPACT_STRING_LEN = INT16
    begin = 0
    end = begin + COMPACT_STRING_LEN
    size_str = int.from_bytes(data[begin:end])
    print("size str", size_str)
    begin = end
    end = size_str + begin
    name = data[begin:end].decode()
    print("name", name)

    begin = end
    end = begin + COMPACT_STRING_LEN
    size_str = int.from_bytes(data[begin:end])
    print("size str", size_str)
    begin = end
    end = size_str + begin
    version = data[begin : end - 1].decode()
    print(data[end - 1 :].decode())

    return ApiVersionsRequest(
        client_software_name=name,
        client_software_version=version,
    )


def api_version_response(header: HeaderRequest) -> bytes:
    def response_bytes(header: HeaderRequest):
        match header.api_version:
            case 0:
                num_api_keys = 2
                return (
                    header.correlation_id.to_bytes(INT32)
                    + ErrorCode.NONE.value.to_bytes(INT16)
                    + num_api_keys.to_bytes(INT8)
                    + api_keys(header.api_key)
                )

            case 2:
                throttle_time_ms = 0
                num_api_keys = 2
                return (
                    header.correlation_id.to_bytes(INT32)
                    + ErrorCode.NONE.value.to_bytes(INT16)
                    + num_api_keys.to_bytes(INT8)
                    + api_keys(header.api_key)
                    + throttle_time_ms.to_bytes(INT32)
                )
            case 3 | 4:
                throttle_time_ms = 0
                tag_buffer = 0
                num_api_keys = 2
                return (
                    header.correlation_id.to_bytes(INT32)
                    + ErrorCode.NONE.value.to_bytes(INT16)
                    + num_api_keys.to_bytes(INT8)
                    + api_keys(header.api_key)
                    + tag_buffer.to_bytes(INT8)
                    + throttle_time_ms.to_bytes(INT32)
                    + tag_buffer.to_bytes(INT8)
                )
            case _:
                return header.correlation_id.to_bytes(
                    INT32
                ) + ErrorCode.UNSUPPORTED_VERSION.value.to_bytes(INT16)

    def api_keys(api_key: int) -> bytes:
        min_version = 0
        max_version = 4
        return (
            api_key.to_bytes(INT16)
            + min_version.to_bytes(INT16)
            + max_version.to_bytes(INT16)
        )

    res_bytes = response_bytes(header)
    msg_size = len(res_bytes)
    return msg_size.to_bytes(INT32) + res_bytes


def accept_client(client: socket.socket):
    while True:
        data = client.recv(1024)
        print("input", data, len(data))
        header = parse_request_header_bytes(data)

        print("header", header)

        response_bytes = api_version_response(header)
        print("output", response_bytes, len(response_bytes))
        client.send(response_bytes)


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        client_socket, _ = server.accept()  # wait for client
        thread = threading.Thread(target=accept_client, args=(client_socket,))
        thread.start()


if __name__ == "__main__":
    main()
