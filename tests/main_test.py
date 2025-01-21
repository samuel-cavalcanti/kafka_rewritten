import unittest
from app import main
from app.api_keys import ApiKeys
from app.header_request import HeaderRequest


class MainTestCase(unittest.TestCase):
    def test_correlation_id(self):
        res = main.correlation_id_respnse(7, 0)

        # msg size 00 00 00 00
        # correlation_id  00 00 00 07
        self.assertEqual(res, b"\x00" * 4 + b"\x00" * 3 + b"\x07")

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
            HeaderRequest(35, api_key=18, api_version=4, correlation_id=1870644833),
            HeaderRequest(35, api_key=18, api_version=26442, correlation_id=1333056139),
            HeaderRequest(35, api_key=18, api_version=4, correlation_id=108694630),
        ]

        for msg, header in zip(msgs, headers):
            result_header = main.parse_request_header_bytes(msg)
            self.assertEqual(result_header, header)

    def test_api_versions_request(self):
        input = b"\x00\x00\x00#\x00\x12\x00\x04\x7fa\xe6\xea\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00"

        header = main.parse_request_header_bytes(input)
        self.assertEqual(header.api_key, ApiKeys.ApiVersions.value.code)
        # main.ApiVersionsRequest(
        #     client_software_name="kafka-cli", client_software_version="kafka-cli40.0.1"
        # )

    def test_api_version_response(self):
        headers = [
            HeaderRequest(
                msg_size=35,
                api_key=18,
                api_version=60035,
                correlation_id=122178114,
            ),
            HeaderRequest(
                msg_size=35,
                api_key=18,
                api_version=4,
                correlation_id=1970255091,
            ),
            HeaderRequest(
                msg_size=35,
                api_key=18,
                api_version=4,
                correlation_id=809767070,
            ),
        ]
        responses = [
            b"\x00\x00\x00\x06\x07HJB\x00#",
            b"\x00\x00\x00\x1auo\xb4\xf3\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x1a0D\x10\x9e\x00\x00\x03\x00\x12\x00\x00\x00\x04\x00\x00K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        ]

        for header, expected_res in zip(headers, responses):
            res = main.kafka_response(header)
            self.assertEqual(res, expected_res)
