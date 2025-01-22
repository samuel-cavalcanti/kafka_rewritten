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
    id = header.correlation_id.to_bytes(INT32)
    if request.cursor is None:
        cursor = NULL.to_bytes(INT8)
    else:
        raise NotImplementedError()

    match header.api_version:
        case 0:
            return throttle_time_ms + topics + cursor + tag_buffer

        case _:
            return id + ErrorCode.UNSUPPORTED_VERSION.value.to_bytes(INT16)


def topic_to_bytes(topic: str) -> bytes:
    size_str = len(topic) + 1
    content = topic.encode()
    tag_buffer = (0).to_bytes(INT8)
    return size_str.to_bytes(COMPACT_STRING_LEN) + content + tag_buffer
