from pathlib import Path
import unittest
from app import main
from app.api_keys.api_version import ApiVersionsRequest
from app.api_keys.describe_topic_partitions import DescribeTopicPartitionsRequest
from app.header_request import HeaderRequest
from app import kafka_parser


class MainTestCase(unittest.TestCase):
    def test_request(self):
        api_version_requests = [
            b"\x00\x00\x00#\x00\x12\x00\x04\nf\xf2\xdf\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
            b"\x00\x00\x00#\x00\x12\x00\x04\xc3W'\xc8\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
            b"\x00\x00\x00#\x00\x12\x00\x04\x7f\xe4\xe4\xa9\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
            b"\x00\x00\x00#\x00\x12\x00\x04-\x99h\xcf\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
            b"\x00\x00\x00#\x00\x12\x00\x040\x9d \x03\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
            b"\x00\x00\x00#\x00\x12\x00\x04T\x0em\x02\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
            b"\x00\x00\x00#\x00\x12\x00\x04Z\x18\xb9\xc5\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
            b"\x00\x00\x00#\x00\x12\x00\x04t\x862r\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
            b"\x00\x00\x00#\x00\x12ZBu\xd1\x99)\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
            b"\x00\x00\x00#\x00\x12\x00\x04]|!\x9a\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
            b"\x00\x00\x00#\x00\x12\x00\x04\x00\x00\x00\x07\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
        ]

        api_version_responses = [
            b"\x00\x00\x00\x1a\nf\xf2\xdf\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x1a\xc3W'\xc8\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x1a\x7f\xe4\xe4\xa9\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x1a-\x99h\xcf\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x1a0\x9d \x03\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x1aT\x0em\x02\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x1aZ\x18\xb9\xc5\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x1at\x862r\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x06u\xd1\x99)\x00#",
            b"\x00\x00\x00\x1a]|!\x9a\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x1a\x00\x00\x00\x07\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        ]

        describe_topics_partitions_requests = [
            b"\x00\x00\x001\x00K\x00\x00[\x90k\xdd\x00\x0ckafka-tester\x00\x02\x12unknown-topic-saz\x00\x00\x00\x00\x01\xff\x00",
            b"\x00\x00\x00#\x00K\x00\x00W\x9a\xd9?\x00\x0ckafka-tester\x00\x02\x04foo\x00\x00\x00\x00\x01\xff\x00",
        ]
        describe_topics_responses = [
            b"\x00\x00\x007[\x90k\xdd\x00\x00\x00\x00\x00\x02\x00\x03\x12unknown-topic-saz\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\r\xf8\x00\xff\x00",
            b"\x00\x00\x00EW\x9a\xd9?\x00\x00\x00\x00\x00\x02\x00\x00\x04foo\x00\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00\x85\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x02\x00\x00\x00\x01\x02\x00\x00\x00\x01\x01\x01\x01\x00\x00\x00\r\xf8\x00\xff\x00",
        ]

        # requests = api_version_requests + describe_topics_partitions_requests
        # responses = api_version_responses + describe_topics_responses
        requests = describe_topics_partitions_requests
        responses = describe_topics_responses

        for request, expecetd_response in zip(requests, responses):
            res = main.kafka_response(request)
            main.CONTEXT = None
            self.assertEqual(res, expecetd_response)

    def test_parse_request_msg(self):
        message_size = b"\x00" * 3 + b"\x23"
        api_key = b"\x00" + b"\x12"
        api_version = b"\x00" + b"\x04"
        correlation_id = b"\x6f" + b"\x7f" + b"\xc6" + b"\x61"

        msgs = [
            message_size + api_key + api_version + correlation_id,
            b"\x00\x00\x00\x23\x00\x12\x67\x4a\x4f\x74\xd2\x8b\x00\x09\x6b\x61\x66\x6b\x61\x2d\x63\x6c\x69\x00\x0a\x6b\x61\x66\x6b\x61\x2d\x63\x6c\x69\x04\x30\x2e\x31\x00",
            b"\x00\x00\x00#\x00\x12\x00\x04\x06z\x8cf\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00",
        ]
        headers = [
            HeaderRequest(
                35,
                api_key=18,
                api_version=4,
                correlation_id=1870644833,
                client_id="",
                tag_buffer=None,
            ),
            HeaderRequest(
                35,
                api_key=18,
                api_version=26442,
                correlation_id=1333056139,
                client_id="kafka-cli",
                tag_buffer=0,
            ),
            HeaderRequest(
                35,
                api_key=18,
                api_version=4,
                correlation_id=108694630,
                client_id="kafka-cli",
                tag_buffer=0,
            ),
        ]

        for msg, header in zip(msgs, headers):
            result_header, _ = kafka_parser.parse_request_header_bytes(msg)
            self.assertEqual(result_header, header)

    def test_api_versions_request(self):
        body_bytes = b"\nkafka-cli\x040.1\x00"
        body_request = kafka_parser.parse_api_version_request(body_bytes)

        expected = ApiVersionsRequest(
            client_software_name="kafka-cli", client_software_version="0.1"
        )
        self.assertEqual(body_request, expected)

    def test_api_version_response(self):
        headers = [
            HeaderRequest(
                msg_size=35,
                api_key=18,
                api_version=60035,
                correlation_id=122178114,
                client_id="",
                tag_buffer=None,
            ),
            HeaderRequest(
                msg_size=35,
                api_key=18,
                api_version=4,
                correlation_id=1970255091,
                client_id="",
                tag_buffer=None,
            ),
            HeaderRequest(
                msg_size=35,
                api_key=18,
                api_version=4,
                correlation_id=809767070,
                client_id="",
                tag_buffer=None,
            ),
        ]
        responses = [
            b"\x00\x00\x00\x06\x07HJB\x00#",
            b"\x00\x00\x00\x1auo\xb4\xf3\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x1a0D\x10\x9e\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        ]

        for header, expected_res in zip(headers, responses):
            res = main.kafka_build_response(header, b"")
            self.assertEqual(res, expected_res)

    def test_DescribeTopicPartitions_response(self):
        inputs = [b"\x02\x12unknown-topic-qux\x00\x00\x00\x00\x01\xff\x00"]
        expected_requests = [
            DescribeTopicPartitionsRequest(
                topics=["unknown-topic-qux"],
                reponse_partition_limit=1,
                cursor=None,
            )
        ]

        for body_bytes, expected in zip(inputs, expected_requests):
            request = kafka_parser.parse_describe_topic_partition_request(body_bytes)
            self.assertEqual(request, expected)

    def test_read_kafka_cluster_log(self):
        log_file = Path(__file__).parent.parent / Path("kafka.log")
        _batchs = kafka_parser.parse_kafka_cluster_log(log_file.read_bytes())
        # for batch in batchs.values():
        #     # print(batch.id)
        #     for record in batch.records:
        #         print(record)

        pass
