import io
import json
import unittest
from pathlib import Path

import fastavro

from ksink.batch import Batch, TopicPartitionPath


class TestBatchSerializer(unittest.TestCase):
    def test_works(self):
        with open(Path(__file__).parent / "resources" / "example.avsc", 'r') as schema_file:
            schema = json.load(schema_file)

        batch = Batch(TopicPartitionPath("test", 1, None))
        batch.write_schema = 1
        schema_cache = {1: fastavro.parse_schema(schema)}

        with open(Path(__file__).parent / "resources" / "example.json", 'r') as data_file:
            data = json.load(data_file)

        with io.BytesIO() as byte_buffer:
            fastavro.schemaless_writer(byte_buffer, schema_cache[1], data)
            batch.records.append(byte_buffer.getvalue())

        actual_bytes = batch.serialise(schema_cache)

        with open(Path(__file__).parent / "resources" / "example.avro", 'rb') as data_file:
            expected_bytes = data_file.read()

        self.assertEqual(expected_bytes, actual_bytes)


if __name__ == '__main__':
    unittest.main()
