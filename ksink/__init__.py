import dataclasses
import logging
from typing import Optional

module_logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Record:
    """Container class for records to be written to a file.

    Args:
        raw_bytes (bytes): The raw bytes to append to the destination.
        schema (str): An optional schema for the record (used in OCF formats).
        parsed_record (dict): The parsed record as a dict, used for creating the file path in the destination.
    """

    raw_bytes: bytes
    schema: Optional[str]
    parsed_record: Optional[dict]
