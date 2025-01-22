from dataclasses import dataclass


@dataclass
class HeaderRequest:
    msg_size: int
    api_key: int
    api_version: int
    correlation_id: int
    client_id: str
