from dataclasses import dataclass
from typing import Optional

from .api_key import ErrorCode

from ..header_request import HeaderRequest

from ..utils import COMPACT_STRING_LEN, INT16, INT32, INT8, NULL, sum_bytes


@dataclass
class DescribeTopicCursor:
    topic_name: str
    partition_index: int


@dataclass
class DescribeTopicPartitionsRequest:
    topics: list[str]
    reponse_partition_limit: int
    cursor: Optional[DescribeTopicCursor]


def describe_topic_partitions_response(
    header: HeaderRequest,
    request: DescribeTopicPartitionsRequest,
) -> bytes:
    tag_buffer = (0).to_bytes(INT8)
    throttle_time_ms = (0).to_bytes(INT32)
    n_topics = len(request.topics) + 1
    topics = n_topics.to_bytes(INT8) + sum_bytes(
        [topic_to_bytes(t) for t in request.topics]
    )
    if request.cursor is None:
        next_cursor = NULL.to_bytes(INT8)
    else:
        next_cursor = cursor_to_bytes(request.cursor)
    match header.api_version:
        case 0:
            return throttle_time_ms + topics + next_cursor + tag_buffer

        case _:
            return ErrorCode.UNSUPPORTED_VERSION.value.to_bytes(INT16)


def topic_to_bytes(topic: str) -> bytes:
    error_code = ErrorCode.UNKNOWN_TOPIC_OR_PARTITION.value.to_bytes(INT16)
    uuid = 0
    name = to_compact_nullable_string(topic)
    """ UUID
        Represents a type 4 immutable universally unique identifier (Uuid).
        The values are encoded using sixteen bytes in network byte order (big-endian).
    """
    topic_id = uuid.to_bytes(INT32 * 4, "big")
    is_internal = (False).to_bytes(INT8)
    partitions = (1).to_bytes(INT8)
    topic_authorized_operations = (0x00000DF8).to_bytes(INT32)

    tag_buffer = (0).to_bytes(INT8)
    return (
        error_code
        + name
        + topic_id
        + is_internal
        + partitions
        + topic_authorized_operations
        + tag_buffer
    )


def cursor_to_bytes(cursor: DescribeTopicCursor) -> bytes:
    topic_name = to_compact_string(cursor.topic_name)
    partition_index = cursor.partition_index.to_bytes(INT32)
    return topic_name + partition_index


def to_compact_nullable_string(string: str) -> bytes:
    if len(string) == 0:
        return NULL.to_bytes(INT8)
    else:
        return to_compact_string(string)


def to_compact_string(string: str) -> bytes:
    size_str = len(string) + 1
    encode_str = string.encode()
    return size_str.to_bytes(INT8) + encode_str
