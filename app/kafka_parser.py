from app.header_request import HeaderRequest
from app.api_keys import ApiVersionsRequest
from .utils import COMPACT_STRING_LEN, INT16, INT32, INT8, NULL, NULLABLE_STRING_LEN
from app.api_keys import (
    DescribeTopicCursor,
    DescribeTopicPartitionsRequest,
)


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
        tag_buffer = int.from_bytes(tag_buffer)
    else:
        tag_buffer = None

    return HeaderRequest(
        int.from_bytes(msg_size),
        int.from_bytes(api_key),
        int.from_bytes(api_version),
        int.from_bytes(correlation_id),
        client_id,
        tag_buffer,
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
) -> DescribeTopicPartitionsRequest:
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
