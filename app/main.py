import socket  # noqa: F401
from dataclasses import dataclass


@dataclass
class HeaderRequest:
    api_key: int
    api_version: int
    correlation_id: int


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
        int.from_bytes(api_key),
        int.from_bytes(api_version),
        int.from_bytes(correlation_id),
    )


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client_socket, address = server.accept()  # wait for client

    data = client_socket.recv(1028)
    print("input", data, len(data))
    header = parse_request_bytes(data)
    mensage_size = 0

    response_bytes = correlation_id_respnse(
        id=header.correlation_id, msg_size=mensage_size
    )

    print("output", response_bytes, len(response_bytes))
    client_socket.send(response_bytes)


if __name__ == "__main__":
    main()
