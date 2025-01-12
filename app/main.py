import socket  # noqa: F401
from dataclasses import dataclass


@dataclass
class HeaderRequest:
    msg_size: int
    api_key: int
    api_version: int
    correlation_id: int


# https://kafka.apache.org/protocol.html#The_Messages_ApiVersions
@dataclass
class ApiVersion4:
    error_code: int
    api_keys: list[int]
    throttle_time_ms: int


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
    error_code = 35
    if 0 <= header.api_version <= 4:
        return (
            header.msg_size.to_bytes(4)
            + header.api_key.to_bytes(2)
            + header.api_version.to_bytes(2)
            + header.correlation_id.to_bytes(4)
        )
    else:
        return (
            header.msg_size.to_bytes(4)
            + header.correlation_id.to_bytes(4)
            + error_code.to_bytes(2)
        )


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client_socket, address = server.accept()  # wait for client

    data = client_socket.recv(1024)
    # mensage_size = int.from_bytes(data)
    # data = data + client_socket.recv(mensage_size - 4)

    print("input", data, len(data))
    header = parse_request_bytes(data)

    print("header", header)

    response_bytes = api_version_response(header)
    client_socket.send(response_bytes)


if __name__ == "__main__":
    main()
