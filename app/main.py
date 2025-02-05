from dataclasses import dataclass
import os
from pathlib import Path
import socket  # noqa: F401
import threading
import sys
from uuid import UUID

from app.api_keys import ErrorCode, ApiKeys
from app.api_keys.api_key import ApiKey
from app.api_keys.api_version import (
    ApiKeysResponse,
    ApiVersionsRequest,
    ApiVersionsResponse,
)
from app.api_keys.describe_topic_partitions import (
    DescribePartitionResponse,
    DescribeTopicPartitionResponse,
    DescribeTopicPartitionsRequest,
    DescribeTopicResponse,
)
from app.header_request import HeaderRequest
from . import api_keys
from .utils import INT16, INT32
from . import kafka_parser


@dataclass
class Context:
    topics: dict[str, UUID]
    topics_partitions: dict[str, list[kafka_parser.PartitionRecordValue]]


def load_metada():
    env_path = os.getenv("KAFKA_LOG")
    if env_path is None:
        path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
    else:
        path = env_path
    kafka_logs = Path(path)

    kafka_log_bytes = kafka_logs.read_bytes()
    batchs = kafka_parser.parse_kafka_cluster_log(kafka_log_bytes)

    topics: dict[str, UUID] = {}

    topics_partitions: dict[str, list[kafka_parser.PartitionRecordValue]] = {}
    feature_levels = {}

    for batch in batchs.values():
        for record in batch.records:
            match record.value:
                case kafka_parser.PartitionRecordValue() as value:
                    key = str(value.topic_uuid)
                    if topics_partitions.get(key) is None:
                        topics_partitions[key] = []
                    topics_partitions[key].append(value)

                case kafka_parser.TopicRecord() as value:
                    topics[value.name] = value.uuid
                case kafka_parser.FeatureLevelRecordValue() as value:
                    feature_levels[value.name] = value.feature_level

    return Context(topics, topics_partitions)


def get_context() -> Context:
    return load_metada()


def describe_topic_partitions(
    header: HeaderRequest,
    body: DescribeTopicPartitionsRequest,
) -> DescribeTopicPartitionResponse:
    context = get_context()

    def to_topic_response(topic_name: str) -> DescribeTopicResponse:
        topic_uuid = context.topics.get(topic_name)

        if topic_uuid is None:
            return DescribeTopicResponse(
                error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                topic=topic_name,
                topic_id=UUID(bytes=(0).to_bytes(INT32 * 4)),
                is_internal=False,
                partitions=[],
                topic_authorized_operations=0x00000DF8,
                tag_buffer=0,
            )

        partitions = context.topics_partitions.get(str(topic_uuid), [])

        def to_partiton_response(
            p: kafka_parser.PartitionRecordValue,
        ) -> DescribePartitionResponse:
            return DescribePartitionResponse(
                error_code=ErrorCode.NONE,
                partition_index=p.id,
                leader_id=p.leader,
                leader_epoch=p.leader_epoch,
                replica_nodes=p.replicas,
                isr_nodes=p.sync_replicas,
                eligible_leader_replicas=[],
                last_known_elr=[],
                offline_replicas=p.removing_replicas,
                tag_buffer=0,
            )

        response_partitions = [to_partiton_response(p) for p in partitions]

        return DescribeTopicResponse(
            error_code=ErrorCode.NONE,
            topic=topic_name,
            topic_id=topic_uuid,
            is_internal=False,
            partitions=response_partitions,
            topic_authorized_operations=0x00000DF8,
            tag_buffer=0,
        )

    topics = [to_topic_response(t) for t in body.topics]

    return DescribeTopicPartitionResponse(
        version=header.api_version,
        tag_buffer=0,
        throttle_time_ms=0,
        topics=topics,
        next_cursor=body.cursor,
    )


def api_versions(
    header: HeaderRequest, body: ApiVersionsRequest
) -> ApiVersionsResponse:
    def api_keys_to_response(k: ApiKey) -> ApiKeysResponse:
        return ApiKeysResponse(
            code=k.code,
            min_version=k.min_version,
            max_version=k.max_version,
            tag_buffer=0,
        )

    keys = [api_keys_to_response(k.value) for k in ApiKeys]

    return ApiVersionsResponse(
        version=header.api_version,
        error_code=ErrorCode.NONE,
        api_keys=keys,
        throttle_time_ms=0,
        tag_buffer=0,
    )


HANDLES = {
    api_keys.ApiKeys.ApiVersions.value.code: (
        kafka_parser.parse_api_version_request,
        api_versions,
    ),
    api_keys.ApiKeys.DescribeTopicPartitions.value.code: (
        kafka_parser.parse_describe_topic_partition_request,
        describe_topic_partitions,
    ),
}


def accept_client(client: socket.socket):
    while True:
        data = client.recv(1024)
        if len(data) == 0:
            break
        try:
            response_bytes = kafka_response(data)
        except Exception as e:
            print(e, file=sys.stderr)
            break

        log = f"input: {data}\noutput: {response_bytes}\n"
        print(log)
        client.sendall(response_bytes)

    print("closing socket")
    client.close()


def kafka_response(data: bytes) -> bytes:
    header, body_bytes = kafka_parser.parse_request_header_bytes(data)
    return kafka_build_response(header, body_bytes)


def kafka_build_response(header: HeaderRequest, body_bytes: bytes):
    res_bytes = header.encode() + kafka_body_response(header, body_bytes)
    msg_size = len(res_bytes)
    return msg_size.to_bytes(INT32) + res_bytes


def kafka_body_response(header: HeaderRequest, body_bytes: bytes) -> bytes:
    handler = HANDLES.get(header.api_key)
    if handler is None:
        error = ErrorCode.UNKNOWN.value.to_bytes(INT16)
        api_key = header.api_key.to_bytes(INT32)
        return api_key + error

    body_parser, callback = handler
    request = body_parser(body_bytes)
    response = callback(header, request)

    return response.encode()


def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        client_socket, _ = server.accept()  # wait for client
        thread = threading.Thread(target=accept_client, args=(client_socket,))
        thread.start()


if __name__ == "__main__":
    main()
