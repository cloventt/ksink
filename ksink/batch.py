import io
import itertools
import secrets
import tempfile
import time
from collections import namedtuple
from pathlib import Path
from typing import Tuple, List, Iterable, Collection

import fastavro
from kafka.consumer.fetcher import ConsumerRecord

from ksink import module_logger

TopicPartitionPath = namedtuple("TopicPartitionPath", ["topic", "partition", "path"])


class Batch:

    _class_logger = module_logger.getChild(__qualname__)

    records: List[bytes] = []

    write_schema = -1

    largest_offset = -1

    bytes_written = 0

    def __init__(self, topic_partition_path: TopicPartitionPath):
        self.log = self._class_logger.getChild(str(id(self)))
        self.topic_partition_path = topic_partition_path
        self.started_at = time.time()

    def __len__(self):
        return len(self.records)

    def append(self, record: ConsumerRecord, schema_id: int):
        if schema_id > self.write_schema:
            self.write_schema = schema_id
        if record.offset > self.largest_offset:
            self.largest_offset = record.offset
        avro_value = record.value[5:]
        self.records.append(avro_value)
        self.bytes_written += len(avro_value)

    def serialise(self, schema_cache: dict) -> bytes:
        """Write the in-memory batch to an Avro file.

        :return: the serialised bytes
        """

        def chunks(it, size):
            it = iter(it)
            return iter(lambda: tuple(itertools.islice(it, size)), ())

        with io.BytesIO() as out_file:
            # first, add the header
            sync_bytes = b'syncbytesyncbyte' #secrets.token_bytes(16)
            fastavro.writer(out_file,
                            schema=schema_cache[self.write_schema],
                            records=[],
                            sync_marker=sync_bytes,
                            )

            self.log.info("Wrote header with sync bytes: %s", sync_bytes)
            # now do the data
            for chunk in chunks(self.records, 100):
                out_file.write(int.to_bytes(len(chunk), 4))  # number of datums in this block
                with io.BytesIO() as buffer:
                    for record_bytes in chunk:
                        buffer.write(record_bytes)
                    buffer.flush()
                    finished_block = buffer.getvalue()
                    out_file.write(int.to_bytes(len(finished_block), 4))  # total size of this block
                    out_file.write(finished_block)  # block data
                    out_file.write(sync_bytes)  # sync bytes to finish the block
                    out_file.flush()

            out_file.seek(0)
            return out_file.getvalue()

    def __repr__(self):
        return f'Batch({self.topic_partition_path}, records={len(self.records)}, size={self.bytes_written} bytes)'
