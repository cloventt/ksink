import multiprocessing
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Lock
from typing import List, Dict

import fastavro
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from schema_registry.client import SchemaRegistryClient

from ksink import module_logger
from ksink.batch import Batch, TopicPartitionPath


class Ksink:
    _class_logger = module_logger.getChild(__qualname__)

    _kafka_consumer: KafkaConsumer = None

    _schema_registry_client: SchemaRegistryClient = None

    _schemas_cache: Dict[int, dict] = dict()

    _live_batches: Dict[TopicPartitionPath, Batch] = dict()

    _live_batch_lock: Lock = Lock()

    _upload_queue = multiprocessing.Queue()

    _threads: List[Thread] = []

    _upload_executor: ThreadPoolExecutor = None

    _running = False

    def __init__(self, config):
        self.log = self._class_logger.getChild(str(id(self)))
        self.config = config

    def run(self):
        self._running = True
        if self._kafka_consumer is None:  # escape hatch for testing
            self._kafka_consumer = KafkaConsumer(**self.config.kafka.consumer,
                                                 enable_auto_commit=False)

        self.log.info("Subscribing to topic: %s", self.config.source.topic)
        self._kafka_consumer.subscribe([self.config.source.topic])
        self.log.info("Subscribed to topic: %s", self.config.source.topic)

        topic_partitions = self._kafka_consumer.partitions_for_topic(self.config.source.topic)

        timeout_thread = Thread(target=self._do_timeouts)
        timeout_thread.daemon = True
        timeout_thread.start()
        self._threads.append(timeout_thread)

        self._schema_registry_client = SchemaRegistryClient(url="http://schema-registry.private.adscale.de")

        self._upload_executor = ThreadPoolExecutor(max_workers=len(topic_partitions))

        for record in self._kafka_consumer:
            # a batch exists per topic-partition to guarantee consistent offset commits
            topic_partition = TopicPartitionPath(record.topic, record.partition, None)
            self.log.debug('Got record: %s, target batch: %s, live batches: %s',
                           record, topic_partition, self._live_batches)
            with self._live_batch_lock:
                if topic_partition not in self._live_batches:
                    self._live_batches[topic_partition] = Batch(topic_partition_path=topic_partition)
                batch = self._live_batches[topic_partition]

            schema_id = int.from_bytes(record.value[1:5])
            if schema_id not in self._schemas_cache:
                schema_from_registry = self._schema_registry_client.get_by_id(schema_id)
                parsed_schema = fastavro.parse_schema(schema_from_registry.raw_schema)
                self.log.info("Successfully retrieved a new schema for ID %s", schema_id)
                self._schemas_cache[schema_id] = parsed_schema

            batch.append(record, schema_id)

            if batch.bytes_written > self.config.batch.size_bytes:
                with self._live_batch_lock:
                    final_batch = self._live_batches.pop(topic_partition)
                    self._upload_executor.submit(self._finalise_batch, final_batch)

    def _finalise_batch(self, batch: Batch):
        self.log.info("Beginning finalisation of batch '%s' at offset %s, beginning with writing the file",
                      batch.topic_partition_path, batch.largest_offset)
        topic_partition = TopicPartition(topic=batch.topic_partition_path.topic,
                                         partition=batch.topic_partition_path.partition)
        self._kafka_consumer.pause(topic_partition)
        self.log.info("Paused %s while current batch is uploaded", batch.topic_partition_path)
        serialised_bytes = batch.serialise()
        self.log.info("File written to disk for %s", batch)
        # TODO: run upload to all configured destinations
        # self._kafka_consumer.commit(offsets={
        #     batch.topic_partition_path: OffsetAndMetadata(batch.largest_offset + 1, None)
        # })
        self.log.info("Committed offsets for %s", batch)
        if self._running:
            self.log.warn("Resuming %s now that in-flight batch is cleared", topic_partition)
            self._kafka_consumer.resume(topic_partition)

    def _do_timeouts(self):
        """Check the current set of batches and time them out if they have passed their expiry.

        :return: None
        """
        while self._running:
            self.log.debug("Timing out expired batches")
            with self._live_batch_lock:
                for topic_partition_path in list(self._live_batches.keys()):
                    batch = self._live_batches.get(topic_partition_path, None)
                    if batch and (time.time() - batch.started_at) > self.config.batch.max_age_seconds:
                        self.log.debug("Timing out an expired batch: %s", batch)
                        final_batch = self._live_batches.pop(topic_partition_path)
                        self._upload_executor.submit(self._finalise_batch, final_batch)
            self.log.debug("Finished timing out expired batches")
            time.sleep(10)

        self.log.info("Finalising all remaining in-flight batches: %s", len(self._live_batches))
        with self._live_batch_lock:
            for topic_partition in list(self._live_batches.keys()):
                final_batch = self._live_batches.pop(topic_partition)
                self._upload_executor.submit(self._finalise_batch, final_batch)

    def close(self):
        if self._kafka_consumer is not None:
            self.log.info('Pausing kafka consumer for shutdown')
            self._kafka_consumer.pause()
        self._running = False
        for thread in self._threads:
            self.log.info("Closing async thread with 30s timeout: %s", thread.name)
            thread.join(30)
        self.log.info("Closing down uploaded, it may take a while for in-flight batches to complete")
        self._upload_executor.shutdown()
        if self._kafka_consumer is not None:
            self.log.info('Closing kafka consumer for shutdown')
            self._kafka_consumer.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.log.info('Shutting down the application')
        self.close()
