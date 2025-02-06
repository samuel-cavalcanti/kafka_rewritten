import socket  # noqa: F401
import threading
import sys

from app.api_keys.api_key import ErrorCode
from app.header_request import HeaderRequest
from .utils import INT16, INT32
from . import kafka_handlers, kafka_parser


def accept_client(client: socket.socket):
    while True:
        data = client.recv(1024)
        if len(data) == 0:
            break
        try:
            response_bytes = kafka_response(data)
        except Exception as e:
            print(e, file=sys.stderr)
            break

        log = f"input: {data}\noutput: {response_bytes}\n"
        print(log)
        client.sendall(response_bytes)

    print("closing socket")
    client.close()


def kafka_response(data: bytes) -> bytes:
    header, body_bytes = kafka_parser.parse_request_header_bytes(data)
    return kafka_build_response(header, body_bytes)


def kafka_build_response(header: HeaderRequest, body_bytes: bytes):
    res_bytes = header.encode() + kafka_body_response(header, body_bytes)
    msg_size = len(res_bytes)
    return msg_size.to_bytes(INT32) + res_bytes


def kafka_body_response(header: HeaderRequest, body_bytes: bytes) -> bytes:
    handlers = kafka_handlers.get_handles()
    handler = handlers.get(header.api_key)
    if handler is None:
        error = ErrorCode.UNKNOWN.value.to_bytes(INT16)
        api_key = header.api_key.to_bytes(INT32)
        return api_key + error

    body_parser, callback = handler
    request = body_parser(body_bytes)
    response = callback(header, request)

    return response.encode()


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        client_socket, _ = server.accept()  # wait for client
        thread = threading.Thread(target=accept_client, args=(client_socket,))
        thread.start()


if __name__ == "__main__":
    main()
