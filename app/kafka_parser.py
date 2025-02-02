from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID
from typing import Callable, Optional
from app.header_request import HeaderRequest
from app.api_keys import ApiVersionsRequest
from .utils import (
    COMPACT_STRING_LEN,
    INT16,
    INT32,
    INT64,
    INT8,
    NULL,
    NULLABLE_STRING_LEN,
)
from app.api_keys import (
    DescribeTopicCursor,
    DescribeTopicPartitionsRequest,
)


def parse_int(data: bytes, type_int: int) -> tuple[int, bytes]:
    value, data = digest(data, type_int)
    return int.from_bytes(value), data


def parse_request_header_bytes(data: bytes) -> tuple[HeaderRequest, bytes]:
    msg_size, data = parse_int(data, INT32)
    api_key, data = parse_int(data, INT16)
    api_version, data = parse_int(data, INT16)
    correlation_id, data = parse_int(data, INT32)
    is_header_v0 = len(data) == 0

    if is_header_v0:
        client_id = ""
    else:
        client_id, data = parse_nullable_string(data)

    is_header_v2 = len(data) != 0

    if is_header_v2:
        tag_buffer, data = parse_int(data, INT8)
    else:
        tag_buffer = None

    return HeaderRequest(
        msg_size,
        api_key,
        api_version,
        correlation_id,
        client_id,
        tag_buffer,
    ), data


def digest(data: bytes, size: int) -> tuple[bytes, bytes]:
    return data[:size], data[size:]


def assert_remain_bytes_is_zero(data: bytes):
    assert len(data) == 0, f"remain bytes: {data}\n"


def parse_nullable_string(data: bytes) -> tuple[str, bytes]:
    size_str, data = parse_int(data, NULLABLE_STRING_LEN)

    string_bytes, data = digest(data, size_str)
    return string_bytes.decode(), data


def parse_compact_string(data: bytes) -> tuple[str, bytes]:
    size_str, data = parse_int(data, COMPACT_STRING_LEN)

    string_bytes, data = digest(data, size_str - 1)
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
    def compact_string_with_tag(body_bytes: bytes) -> tuple[str, bytes]:
        name, body_bytes = parse_compact_string(body_bytes)
        tag_buffer, body_bytes = digest(body_bytes, INT8)
        return name, body_bytes

    topics, body_bytes = parse_compact_array(body_bytes, compact_string_with_tag)

    reponse_partition_limit, body_bytes = parse_int(body_bytes, INT32)

    if body_bytes[0] == NULL:
        cursor = None
    else:
        topic_name, body_bytes = parse_compact_string(body_bytes)
        partition_index, body_bytes = parse_int(body_bytes, INT32)
        tag_buffer, body_bytes = digest(body_bytes, INT8)
        cursor = DescribeTopicCursor(
            topic_name=topic_name,
            partition_index=partition_index,
        )

    return DescribeTopicPartitionsRequest(
        topics=topics,
        reponse_partition_limit=reponse_partition_limit,
        cursor=cursor,
    )


@dataclass
class BatchRecords:
    id: int
    records: list[Record]
    base_timestamp: int
    max_timestamp: int
    producer: Optional[Producer]


@dataclass
class Producer:
    id: int
    epoch: int


def parse_kafka_cluster_log(data: bytes) -> dict[int, BatchRecords]:
    batchs = {}
    while len(data) != 0:
        batch_id, data = parse_int(data, INT64)
        batch_length, data = parse_int(data, INT32)

        batch_bytes, data = digest(data, batch_length)

        assert len(batch_bytes) == batch_length
        batch = parse_batch_record(batch_bytes, batch_id)
        batchs[batch.id] = batch
    return batchs


def parse_batch_record(data: bytes, id: int):
    partition_leader_epoch, data = digest(data, INT32)
    magic_byte, data = digest(data, INT8)
    crc, data = digest(data, INT32)
    attributes, data = digest(data, INT16)
    last_offset_delta, data = digest(data, INT32)
    base_timestamp, data = parse_int(data, INT64)
    max_timestamp, data = parse_int(data, INT64)

    producer, data = parser_batch_producer(data)
    base_sequence, data = digest(data, INT32)
    n_records, data = parse_int(data, INT32)

    records = []

    for _ in range(n_records):
        record_length, data = parse_varint(data)
        record_length = zigzag_decode(record_length)
        record_bytes, data = digest(data, record_length)

        assert (
            len(record_bytes) == record_length
        ), f"record length {record_length} {len(record_bytes)}"
        record = parse_record(record_bytes)
        records.append(record)

    return BatchRecords(
        id,
        records,
        base_timestamp,
        max_timestamp,
        producer,
    )


def parser_batch_producer(data: bytes) -> tuple[Optional[Producer], bytes]:
    producer_id, data = parse_int(data, INT64)
    producer_epoch, data = parse_int(data, INT16)

    not_set_id = b"\xff" * INT64
    not_set_epoch = b"\xff" * INT16
    not_set = not_set_id + not_set_epoch

    if producer_id + producer_epoch == not_set:
        producer = None
    else:
        producer = Producer(producer_id, producer_epoch)

    return producer, data


@dataclass
class RecordValue:
    frame_version: int
    version: int


@dataclass
class PartitionRecordValue(RecordValue):
    id: int
    topic_uuid: UUID
    replicas: list[int]
    sync_replicas: list[int]
    removing_replicas: list[int]
    adding_replicas: list[int]
    leader: int
    leader_epoch: int
    partition_epoch: int
    directories: list[UUID]


@dataclass
class FeatureLevelRecordValue(RecordValue):
    name: str
    feature_level: int


@dataclass
class TopicRecord(RecordValue):
    name: str
    uuid: UUID


# RecordValue = FeatureLevelRecordValue | TopicRecord | PartitionRecordValue


@dataclass
class Record:
    timestamp_delta: int
    value: RecordValue


def parse_record(data: bytes) -> Record:
    attributes, data = digest(data, INT8)
    timestamp_delta, data = parse_int(data, INT8)
    offset_delta, data = digest(data, INT8)
    record_key, data = parse_record_key(data)

    length, data = parse_varint(data)
    length = zigzag_decode(length)
    value_bytes, data = digest(data, length)

    # print(", ".join([hex(b) for b in data]))
    record_value = parse_record_value(value_bytes)
    headers_array_count, data = digest(data, INT8)
    assert_remain_bytes_is_zero(data)

    return Record(timestamp_delta, record_value)


def parse_record_value(value_bytes: bytes) -> RecordValue:
    frame_version, value_bytes = parse_int(value_bytes, INT8)
    type_record_value, value_bytes = parse_int(value_bytes, INT8)
    version_record_value, value_bytes = parse_int(value_bytes, INT8)
    rec = RecordValue(frame_version, version_record_value)

    match type_record_value:
        case 12:
            return parse_feature_level_record_value(value_bytes, rec)
        case 2:
            return parse_topic_record_value(value_bytes, rec)
        case 3:
            return parse_partition_record_value(value_bytes, rec)
        case _:
            raise NotImplementedError(f"type record {type_record_value}, {rec}")


def parse_compact_array[T](
    data: bytes, callback: Callable[[bytes], tuple[T, bytes]]
) -> tuple[list[T], bytes]:
    length, data = parse_int(data, INT8)

    array = []
    for _ in range(length - 1):
        value, data = callback(data)
        array.append(value)

    return array, data


def parse_compact_arrayi32(data: bytes) -> tuple[list[int], bytes]:
    return parse_compact_array(data, lambda d: parse_int(d, INT32))


def parse_compact_arrayuuid(data: bytes) -> tuple[list[UUID], bytes]:
    return parse_compact_array(data, parse_uuid)


def parse_partition_record_value(data: bytes, rec: RecordValue) -> PartitionRecordValue:
    id, data = parse_int(data, INT32)
    toppic_uuid, data = parse_uuid(data)

    replicas, data = parse_compact_arrayi32(data)
    sync_replicas, data = parse_compact_arrayi32(data)
    removing_replicas, data = parse_compact_arrayi32(data)
    adding_replicas, data = parse_compact_arrayi32(data)
    leader, data = parse_int(data, INT32)
    leader_epoch, data = parse_int(data, INT32)
    partition_epoch, data = parse_int(data, INT32)
    directories, data = parse_compact_array(data, parse_uuid)
    tagged_fields_count, data = digest(data, INT8)
    assert tagged_fields_count == b"\x00"

    return PartitionRecordValue(
        rec.frame_version,
        rec.version,
        id,
        toppic_uuid,
        replicas,
        sync_replicas,
        removing_replicas,
        adding_replicas,
        leader,
        leader_epoch,
        partition_epoch,
        directories,
    )


def parse_feature_level_record_value(
    data: bytes, rec: RecordValue
) -> FeatureLevelRecordValue:
    name, data = parse_compact_string(data)
    feature_level, data = parse_int(data, INT16)
    taggeed_field_count, data = parse_int(data, INT8)

    assert taggeed_field_count == 0

    assert_remain_bytes_is_zero(data)
    return FeatureLevelRecordValue(
        rec.frame_version,
        rec.version,
        name,
        feature_level,
    )


def parse_topic_record_value(data: bytes, rec: RecordValue) -> TopicRecord:
    name, data = parse_compact_string(data)
    uuid, data = parse_uuid(data)
    taggeed_field_count, data = digest(data, INT8)

    assert_remain_bytes_is_zero(data)

    return TopicRecord(rec.frame_version, rec.version, name, uuid)


def parse_uuid(data: bytes) -> tuple[UUID, bytes]:
    uuid, data = digest(data, INT64 * 2)

    return UUID(bytes=uuid), data


def zigzag_decode(n: int) -> int:
    return (n >> 1) ^ (-(n & 1))


def parse_varint(data: bytes) -> tuple[int, bytes]:
    def parse_byte(data: bytes) -> tuple[str, bytes]:
        var_int_bytes, data = parse_int(data, INT8)
        string_bits = bin(var_int_bytes)
        string_bits = string_bits[2:].zfill(8)
        return string_bits, data

    first_string, data = parse_byte(data)
    result_string = first_string[1:]
    while first_string[0] == "1":
        first_string, data = parse_byte(data)
        result_string = first_string[1:] + result_string

    return int(result_string, 2), data


def parse_record_key(data: bytes) -> tuple[Optional[str], bytes]:
    null = -1

    key_length, data = parse_int(data, INT8)
    length = zigzag_decode(key_length)

    if length == null:
        return None, data

    raise NotImplementedError(f"key length {key_length}")
