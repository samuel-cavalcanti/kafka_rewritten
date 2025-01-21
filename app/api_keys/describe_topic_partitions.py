from dataclasses import dataclass

from ..utils import INT16, INT32, INT8


@dataclass
class DescribeTopicCursor:
    topic_name: str
    partition_index: int


class DescribeTopicPartitionsRequest:
    topics: list[str]
    reponse_partition_limit: int
    cursor: DescribeTopicCursor


def describe_topic_partitions_response(
    request: DescribeTopicPartitionsRequest,
) -> bytes:
    tag_buffer = (0).to_bytes(INT8)
    throttle_time_ms = (0).to_bytes(INT32)

    # return throttle_time_ms + topics + next_cursor + tag_buffer

    return b""
