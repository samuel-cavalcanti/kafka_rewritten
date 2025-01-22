import socket  # noqa: F401
import threading

from app.api_keys.api_key import ErrorCode
from app.api_keys import ApiVersionsRequest
from app.api_keys.describe_topic_partitions import (
    DescribeTopicCursor,
    DescribeTopicPartitionsRequest,
)
from app.header_request import HeaderRequest
from . import api_keys
from .utils import COMPACT_STRING_LEN, INT16, INT32, INT8, NULL, NULLABLE_STRING_LEN


def correlation_id_respnse(id: int, msg_size: int) -> bytes:
    # The correlation_id field is a 32-bit signed integer.
    # The message_size field is a 32-bit signed integer
    bytes_len = 4  # 32/4
    response_bytes = msg_size.to_bytes(bytes_len) + id.to_bytes(bytes_len)
    return response_bytes


def parse_request_header_bytes(data: bytes) -> tuple[HeaderRequest, bytes]:
    msg_size, data = digest(data, INT32)
    api_key, data = digest(data, INT16)
    api_version, data = digest(data, INT16)
    correlation_id, data = digest(data, INT32)
    is_header_v0 = len(data) == 0

    if is_header_v0:
        client_id = ""
    else:
        client_id, data = parse_nullable_string(data)

    is_header_v2 = len(data) != 0

    if is_header_v2:
        tag_buffer, data = digest(data, INT8)

    return HeaderRequest(
        int.from_bytes(msg_size),
        int.from_bytes(api_key),
        int.from_bytes(api_version),
        int.from_bytes(correlation_id),
        client_id,
    ), data


def digest(data: bytes, size: int) -> tuple[bytes, bytes]:
    return data[:size], data[size:]


def parse_nullable_string(data: bytes) -> tuple[str, bytes]:
    size_str, data = digest(data, NULLABLE_STRING_LEN)
    size_str = int.from_bytes(size_str)

    string_bytes, data = digest(data, size_str)
    return string_bytes.decode(), data


def parse_compact_string(data: bytes) -> tuple[str, bytes]:
    size_str, data = digest(data, COMPACT_STRING_LEN)
    size_str = int.from_bytes(size_str) - 1

    string_bytes, data = digest(data, size_str)
    return string_bytes.decode(), data


def parse_api_version_request(data: bytes) -> ApiVersionsRequest:
    name, data = parse_compact_string(data)
    version, data = parse_compact_string(data)
    tag_buffer = digest(data, INT8)

    return ApiVersionsRequest(
        client_software_name=name,
        client_software_version=version,
    )


def parse_describe_topic_partition_request(
    body_bytes: bytes,
) -> api_keys.DescribeTopicPartitionsRequest:
    num_topics, body_bytes = digest(body_bytes, INT8)
    num_topics = int.from_bytes(num_topics) - 1
    topics = []
    for _ in range(num_topics):
        name, body_bytes = parse_compact_string(body_bytes)
        tag_buffer, body_bytes = digest(body_bytes, INT8)
        topics.append(name)

    reponse_partition_limit, body_bytes = digest(body_bytes, INT32)

    if body_bytes[0] == NULL:
        cursor = None
    else:
        topic_name, body_bytes = parse_compact_string(body_bytes)
        partition_index, body_bytes = digest(body_bytes, INT32)
        tag_buffer, body_bytes = digest(body_bytes, INT8)
        cursor = DescribeTopicCursor(
            topic_name=topic_name,
            partition_index=int.from_bytes(partition_index),
        )

    return DescribeTopicPartitionsRequest(
        topics=topics,
        reponse_partition_limit=int.from_bytes(reponse_partition_limit),
        cursor=cursor,
    )


def kafka_response(data: bytes) -> bytes:
    header, body_bytes = parse_request_header_bytes(data)
    return kafka_build_response(header, body_bytes)


def kafka_build_response(header: HeaderRequest, body_bytes: bytes):
    res_bytes = kafka_parse(header, body_bytes)
    msg_size = len(res_bytes)
    return msg_size.to_bytes(INT32) + res_bytes


def kafka_parse(header: HeaderRequest, body_bytes: bytes) -> bytes:
    match header.api_key:
        case api_keys.ApiKeys.ApiVersions.value.code:
            return api_keys.api_version_response(header)
        case api_keys.ApiKeys.DescribeTopicPartitions.value.code:
            request = parse_describe_topic_partition_request(body_bytes)
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
