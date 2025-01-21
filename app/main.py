import socket  # noqa: F401
from dataclasses import dataclass
import threading

from app.api_keys.api_key import ErrorCode
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
    begin = COMPACT_STRING_LEN + len(name)
    version = parse_compact_string(data[begin:])

    return ApiVersionsRequest(
        client_software_name=name,
        client_software_version=version,
    )


def kafka_response(header: HeaderRequest) -> bytes:
    def dasdas(header: HeaderRequest) -> bytes:
        match header.api_key:
            case api_keys.ApiKeys.ApiVersions.value.code:
                return api_keys.api_version_response(header)
            # case api_keys.ApiKeys.DescribeTopicPartitions.value.code:
            #
            #     return api_keys.describe_topic_partitions_response()
            case _:
                return header.api_key.to_bytes(
                    INT32
                ) + ErrorCode.UNKNOWN.value.to_bytes(INT16)

    res_bytes = dasdas(header)
    msg_size = len(res_bytes)
    return msg_size.to_bytes(INT32) + res_bytes


def accept_client(client: socket.socket):
    while True:
        data = client.recv(1024)
        if len(data) == 0:
            break
        print(f"input {data} {len(data)}")
        try:
            header = parse_request_header_bytes(data)

            print(f"header {header}")
            response_bytes = kafka_response(header)

        except Exception as e:
            print(e)
            break

        print(f"output {response_bytes} {len(response_bytes)}")
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
