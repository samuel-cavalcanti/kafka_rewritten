from dataclasses import dataclass
from ..utils import INT16, INT32, INT8, sum_bytes
from .api_key import ApiKeys, ApiKey, ErrorCode
from ..header_request import HeaderRequest


def api_key_to_bytes(api_key: ApiKey) -> bytes:
    min_version = api_key.min_version
    max_version = api_key.max_version
    return (
        api_key.code.to_bytes(INT16)
        + min_version.to_bytes(INT16)
        + max_version.to_bytes(INT16)
    )


@dataclass
class ApiVersionsRequest:
    client_software_name: str
    client_software_version: str


def api_version_response(header: HeaderRequest):
    assert header.api_key == ApiKeys.ApiVersions.value.code

    tag_buffer = (0).to_bytes(INT8)
    throttle_time_ms = (0).to_bytes(INT32)

    if header.api_version <= 2:
        supported_api_keys = [api_key_to_bytes(key.value) for key in ApiKeys]
    else:
        supported_api_keys = [
            api_key_to_bytes(key.value) + tag_buffer for key in ApiKeys
        ]

    num_api_keys = len(supported_api_keys) + 1
    api_keys = num_api_keys.to_bytes(INT8, signed=True) + sum_bytes(supported_api_keys)

    match header.api_version:
        case 0:
            return ErrorCode.NONE.value.to_bytes(INT16) + api_keys

        case 1 | 2:
            return ErrorCode.NONE.value.to_bytes(INT16) + api_keys + throttle_time_ms
        case 3 | 4:
            return (
                ErrorCode.NONE.value.to_bytes(INT16)
                + api_keys
                + throttle_time_ms
                + tag_buffer
            )
        case _:
            return ErrorCode.UNSUPPORTED_VERSION.value.to_bytes(INT16)
