import socket  # noqa: F401
from dataclasses import dataclass
import threading

from app.header_request import HeaderRequest
from . import api_keys
from .utils import INT16, INT32


@dataclass
class ApiVersionsRequest:
    client_software_name: str
    client_software_version: str


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


COMPACT_STRING_LEN = INT16


def parse_compact_string(data: bytes) -> str:
    size_str = int.from_bytes(data[0:COMPACT_STRING_LEN])
    string_bytes = data[COMPACT_STRING_LEN : COMPACT_STRING_LEN + size_str]
    return string_bytes.decode()


def parse_api_version_request(data: bytes) -> ApiVersionsRequest:
    name = parse_compact_string(data)
    print("name", name)
    begin = COMPACT_STRING_LEN + len(name)
    version = parse_compact_string(data[begin:])
    print("version", version)

    return ApiVersionsRequest(
        client_software_name=name,
        client_software_version=version,
    )


def kafka_response(header: HeaderRequest) -> bytes:
    if header.api_key == api_keys.ApiKeys.ApiVersions.value.code:
        res_bytes = api_keys.api_version_response(header)
        msg_size = len(res_bytes)
        return msg_size.to_bytes(INT32) + res_bytes
    raise ValueError(f"API key not supported: {header.api_key}")


def accept_client(client: socket.socket):
    while True:
        data = client.recv(1024)
        if len(data) == 0:
            return

        header = parse_request_header_bytes(data)

        response_bytes = kafka_response(header)

        client.sendall(response_bytes)

        log = f"input {data} {len(data)}\n"
        log += f"header {header}\n"
        log += f"output {response_bytes} {len(response_bytes)}"
        print(log)


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
