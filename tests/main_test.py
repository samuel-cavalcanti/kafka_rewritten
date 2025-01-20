import unittest
from app import main
from app.api_keys import ApiKeys


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
            main.HeaderRequest(
                35, api_key=18, api_version=4, correlation_id=1870644833
            ),
            main.HeaderRequest(
                35, api_key=18, api_version=26442, correlation_id=1333056139
            ),
            main.HeaderRequest(35, api_key=18, api_version=4, correlation_id=108694630),
        ]

        for msg, header in zip(msgs, headers):
            result_header = main.parse_request_header_bytes(msg)
            self.assertEqual(result_header, header)

    def test_api_versions_request(self):
        input = b"\x00\x00\x00#\x00\x12\x00\x04\x7fa\xe6\xea\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00"

        header = main.parse_request_header_bytes(input)
        self.assertEqual(header.api_key, ApiKeys.ApiVersions.value)
        body_bytes = input[8 + 4 :]
        print(body_bytes[:4])

        body = main.parse_api_version_request(body_bytes)
        print(body)

        self.assertEqual(True, False)

    def test_api_version_response(self):
        headers = [
            main.HeaderRequest(
                35,
                api_key=18,
                api_version=26442,
                correlation_id=1333056139,
            ),
            main.HeaderRequest(35, api_key=18, api_version=4, correlation_id=869853728),
        ]
        responses = [
            b"\x00\x00\x00\x06Ot\xd2\x8b\x00#",
            b"\x00\x00\x00\x143\xd8\xea \x00\x00\x00\x12\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00",
        ]

        for header, expected_res in zip(headers, responses):
            res = main.kafka_response(header)
            self.assertEqual(res, expected_res)
