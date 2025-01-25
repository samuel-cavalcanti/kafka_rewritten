import socket  # noqa: F401
import threading
import sys

from app.api_keys import ErrorCode, ApiKeys
from app.header_request import HeaderRequest
from . import api_keys
from .utils import INT16, INT32, INT8
from . import kafka_parser





def kafka_response(data: bytes) -> bytes:
    header, body_bytes = kafka_parser.parse_request_header_bytes(data)
    return kafka_build_response(header, body_bytes)


def kafka_build_response(header: HeaderRequest, body_bytes: bytes):
    res_bytes = kafka_header_response(header) + kafka_body_response(header, body_bytes)
    msg_size = len(res_bytes)
    return msg_size.to_bytes(INT32) + res_bytes


def kafka_header_response(header: HeaderRequest) -> bytes:
    id = header.correlation_id.to_bytes(INT32)
    if header.api_key != ApiKeys.ApiVersions.value.code:
        tag_buffer = (0).to_bytes(INT8)
        return id + tag_buffer

    return id


def kafka_body_response(header: HeaderRequest, body_bytes: bytes) -> bytes:
    match header.api_key:
        case api_keys.ApiKeys.ApiVersions.value.code:
            request = kafka_parser.parse_api_version_request(body_bytes)
            return api_keys.api_version_response(header)
        case api_keys.ApiKeys.DescribeTopicPartitions.value.code:
            request = kafka_parser.parse_describe_topic_partition_request(body_bytes)
            return api_keys.describe_topic_partitions_response(header, request)
        case _:
            return header.api_key.to_bytes(INT32) + ErrorCode.UNKNOWN.value.to_bytes(
                INT16
            )


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

        log = f"input: {data}\noutput: {response_bytes}"
        print(log)
        client.sendall(response_bytes)

    print("closing socket")
    client.close()


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
