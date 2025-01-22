from dataclasses import dataclass
from typing import Optional


@dataclass
class HeaderRequest:
    msg_size: int
    api_key: int
    api_version: int
    correlation_id: int
    client_id: str
    tag_buffer: Optional[int]
