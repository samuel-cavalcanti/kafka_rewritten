from __future__ import annotations
from dataclasses import dataclass
from uuid import UUID

from app.utils import (
    INT32,
    INT64,
    encode_compact_array,
    encode_error_code,
    encode_tag_buffer,
)


# https://kafka.apache.org/protocol.html#The_Messages_Fetch


@dataclass
class FetchRequest_V17:
    max_wait_ms: int
    min_bytes: int
    max_bytes: int
    isolation_level: int
    session_id: int
    session_epoch: int
    topics: FetchRequest_V17Topics
    forgotten_topics_data: FetchForgottenTopics
    rack_id: str
    tag_buffer: int


@dataclass
class FetchRequest_V17Topics:
    topic_id: UUID
    partitions: list[FetchRequest_V17Partition]
    tag_buffer: int


@dataclass
class FetchRequest_V17Partition:
    partition: int
    current_leader_epoch: int
    fetch_offset: int
    last_fetched_epoch: int
    log_start_offset: int
    partition_max_bytes: int


@dataclass
class FetchForgottenTopics:
    topic_id: UUID
    partitions: list[int]
    tag_buffer: int


@dataclass
class FetchResponse_V17:
    throttle_time_ms: int
    error_code: int
    session_id: int
    responses: list[FetchResponse_V17]
    tag_buffer: int

    def encode(self) -> bytes:
        throtle_time_ms = self.throttle_time_ms.to_bytes(INT32)
        error_code = encode_error_code(self.error_code)
        session_id = self.session_id.to_bytes(INT32)
        responses = encode_compact_array(self.responses, lambda r: r.encode())
        tag = encode_tag_buffer(self.tag_buffer)
        return throtle_time_ms + error_code + session_id + responses + tag


@dataclass
class FetchResponses_v17:
    topic_id: UUID
    partitions: list[FetchPartitonResponse]

    def encode(self) -> bytes:
        partitions = encode_compact_array(self.partitions, lambda p: p.encode())
        return self.topic_id.bytes + partitions


@dataclass
class FetchPartitonResponse:
    partition_index: int
    error_code: int
    high_watermark: int
    last_stable_offset: int
    log_start_offset: int
    aborted_transactions: FetchAbortedTransaction
    preferred_read_replica: int
    records: list[int]  # COMPACT_RECORDS

    def encode(self) -> bytes:
        raise NotImplementedError


@dataclass
class FetchAbortedTransaction:
    producer_id: int
    first_offset: int

    def encode(self) -> bytes:
        id = self.producer_id.to_bytes(INT64)
        offset = self.first_offset.to_bytes(INT64)
        return id + offset
