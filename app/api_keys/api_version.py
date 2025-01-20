from ..utils import INT16, INT32, INT8
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


def api_version_response(header: HeaderRequest):
    assert header.api_key == ApiKeys.ApiVersions.value.code

    match header.api_version:
        case 0:
            supported_api_keys = [api_key_to_bytes(key.value) for key in ApiKeys]
            num_api_keys = len(supported_api_keys) + 1
            supported_api_keys = b"".join(supported_api_keys)
            return (
                header.correlation_id.to_bytes(INT32)
                + ErrorCode.NONE.value.to_bytes(INT16)
                + num_api_keys.to_bytes(INT8)
                + supported_api_keys
            )

        case 2:
            supported_api_keys = [api_key_to_bytes(key.value) for key in ApiKeys]
            num_api_keys = len(supported_api_keys) + 1
            supported_api_keys = b"".join(supported_api_keys)
            throttle_time_ms = 0
            return (
                header.correlation_id.to_bytes(INT32)
                + ErrorCode.NONE.value.to_bytes(INT16)
                + num_api_keys.to_bytes(INT8)
                + supported_api_keys
                + throttle_time_ms.to_bytes(INT32)
            )
        case 3 | 4:
            throttle_time_ms = 0
            tag_buffer = 0
            supported_api_keys = [api_key_to_bytes(key.value) for key in ApiKeys]
            num_api_keys = len(supported_api_keys) + 1
            keys_bytes = supported_api_keys[0] + tag_buffer.to_bytes(INT8)
            for api_key_bytes in supported_api_keys:
                keys_bytes = keys_bytes + api_key_bytes + tag_buffer.to_bytes(INT8)

            return (
                header.correlation_id.to_bytes(INT32)
                + ErrorCode.NONE.value.to_bytes(INT16)
                + num_api_keys.to_bytes(INT8)
                + keys_bytes
                # + tag_buffer.to_bytes(INT8)
                + throttle_time_ms.to_bytes(INT32)
                + tag_buffer.to_bytes(INT8)
            )
        case _:
            return header.correlation_id.to_bytes(
                INT32
            ) + ErrorCode.UNSUPPORTED_VERSION.value.to_bytes(INT16)
