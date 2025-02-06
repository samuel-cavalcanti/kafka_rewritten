from dataclasses import dataclass
from uuid import UUID


# https://kafka.apache.org/protocol.html#The_Messages_Fetch

@dataclass
class FetchRequest_V17Partition:
    partition: int
    current_leader_epoch: int
    fetch_offse: int
    last_fetched_epoch: int
    log_start_offse: int
    partition_max_byte: int


@dataclass
class FetchRequest_V17Topics:
    topic_id: UUID
    partitions: list[FetchRequest_V17Partition]
    tag_buffer: int


@dataclass
class FetchRequest_V17:
    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    session_id: int
    session_epoch: int
    topics: FetchRequest_V17Topics
    forgotten_topics_data: list[FetchRequest_V17Partition]
    rack_id: int
    tag_buffer: int
