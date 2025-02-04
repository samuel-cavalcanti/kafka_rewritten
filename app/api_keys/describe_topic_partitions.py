from uuid import UUID
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


@dataclass
class DescribeTopicResponse:
    error_code: ErrorCode
    topic: str
    topic_id: UUID
    is_internal: bool
    partitions: list[str]
    topic_authorized_operations: int
    tag_buffer: int

    def encode(self) -> bytes:
        error_code = self.error_code.value.to_bytes(INT16)
        name = to_compact_nullable_string(self.topic)
        is_internal = self.is_internal.to_bytes(INT8)
        if len(self.partitions) == 0:
            partitions = (1).to_bytes(INT8)
        else:
            raise NotImplementedError(f"{self.partitions}")
        topic_authorized_operations = self.topic_authorized_operations.to_bytes(INT32)

        tag_buffer = self.tag_buffer.to_bytes(INT8)
        return (
            error_code
            + name
            + self.topic_id.bytes
            + is_internal
            + partitions
            + topic_authorized_operations
            + tag_buffer
        )


@dataclass
class DescribeTopicPartitionResponse:
    version: int
    tag_buffer: int
    throttle_time_ms: int
    topics: list[DescribeTopicResponse]
    next_cursor: Optional[DescribeTopicCursor]

    def encode(self) -> bytes:
        tag_buffer = self.tag_buffer.to_bytes(INT8)
        throttle_time_ms = self.throttle_time_ms.to_bytes(INT32)
        n_topics = len(self.topics) + 1

        topics = n_topics.to_bytes(INT8) + sum_bytes([t.encode() for t in self.topics])
        if self.next_cursor is None:
            next_cursor = NULL.to_bytes(INT8)
        else:
            next_cursor = cursor_to_bytes(self.next_cursor)
        match self.version:
            case 0:
                return throttle_time_ms + topics + next_cursor + tag_buffer
            case _:
                return ErrorCode.UNSUPPORTED_VERSION.value.to_bytes(INT16)


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
