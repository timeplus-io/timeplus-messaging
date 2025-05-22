"""
Timeplus consumer implementation with single and multi-topic variants
"""

import json
import time
import threading
import logging
from typing import Dict, List, Optional, Union, Callable, Any
from queue import Queue, Empty
import uuid
from abc import ABC, abstractmethod

from proton_driver import client
from timeplus_messaging.record import TimeplusRecord


class ConsumerException(Exception):
    """Exception raised by the consumer"""

    pass


class TimeplusConsumer(ABC):
    """Base abstract class for Timeplus consumers"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8463,
        user: str = "default",
        password: str = "",
        database: str = "default",
        group_id: Optional[str] = None,
        auto_offset_reset: str = "latest",
        partition: int | None = None,
        **kwargs,
    ):
        """Initialize the Timeplus consumer"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.group_id = group_id or f"timeplus_consumer_{uuid.uuid4().hex[:8]}"
        self.auto_offset_reset = auto_offset_reset
        self.partition = partition

        # Client for metadata operations
        self.client = client.Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            **kwargs,
        )

        # Consumer state
        self._closed = False
        self._paused = False
        self._pause_condition = threading.Condition()
        self._consumer_thread = None
        self._message_queue = Queue()
        self._logger = logging.getLogger(self.__class__.__name__)
        self._running = False

        # Thread tracking
        self._topic_threads = {}

        # Offset tracking
        self._commit_offsets = {}

    def _consume_stream(self, topic: str):
        """Internal method to consume from a stream"""
        try:
            # Determine the starting point based on auto_offset_reset
            if self.partition is None:
                if self.auto_offset_reset == "earliest":
                    query = f"SELECT _tp_time, _key, _value, _headers, _tp_sn, _tp_shard FROM {topic} WHERE _tp_time >= earliest_ts()"
                    self._logger.debug(
                        f"Consume stream {topic} from earliest sql: {query}"
                    )
                else:  # latest
                    query = f"SELECT _tp_time, _key, _value, _headers, _tp_sn, _tp_shard FROM {topic} WHERE _tp_time >= now()"
                    self._logger.debug(
                        f"Consume stream {topic} from latest, sql: {query}"
                    )
            else:
                if self.auto_offset_reset == "earliest":
                    query = f"SELECT _tp_time, _key, _value, _headers, _tp_sn, _tp_shard FROM {topic} WHERE _tp_time >= earliest_ts() and _tp_shard = {self.partition}"
                    self._logger.debug(
                        f"Consume stream {topic} from earliest sql: {query}"
                    )
                else:  # latest
                    query = f"SELECT _tp_time, _key, _value, _headers, _tp_sn, _tp_shard FROM {topic} WHERE _tp_time >= now() and _tp_shard = {self.partition}"
                    self._logger.debug(
                        f"Consume stream {topic} from latest, sql: {query}"
                    )

            # Use execute_iter for streaming consumption
            rows = self.client.execute_iter(query)
            for row in rows:
                if self._closed:
                    break

                # Proper pause mechanism using condition variable
                with self._pause_condition:
                    while self._paused and not self._closed:
                        self._logger.debug(
                            f"Consumer paused, waiting for resume - topic: {topic}"
                        )
                        self._pause_condition.wait()

                    if self._closed:
                        break

                self._logger.debug(f"Get one row from {topic}")
                # Parse the row
                timestamp, key, value, headers_str, sn, partition = row

                # Convert timestamp to milliseconds
                ts_ms = int(timestamp.timestamp() * 1000) if timestamp else None

                # Parse headers
                try:
                    headers = json.loads(headers_str) if headers_str else {}
                except json.JSONDecodeError:
                    headers = {}

                # Try to parse value as JSON
                try:
                    parsed_value = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    parsed_value = value

                # Create record
                record = TimeplusRecord(
                    topic=topic,
                    value=parsed_value,
                    key=key if key else None,
                    offset=sn,
                    partition=partition,
                    timestamp=ts_ms,
                    headers=headers,
                )

                # Check if we have a callback for this topic
                callback = (
                    self._topic_callbacks.get(topic)
                    if hasattr(self, "_topic_callbacks")
                    else None
                )
                if callback:
                    try:
                        # Call the callback with the record
                        callback(record)
                    except Exception as e:
                        self._logger.error(f"Error in callback for topic {topic}: {e}")

                # Add to queue
                self._message_queue.put(record)

                self._commit_offsets[topic] = (
                    sn  # TODO: commit offset to Timeplus stream, so it can resume from that offset
                )

        except Exception as e:
            self._logger.error(f"Error consuming from {topic}: {e}")
            if not self._closed:
                # Re-raise if not closed intentionally
                raise ConsumerException(f"Consumption failed: {e}")

    @abstractmethod
    def _consumer_loop(self):
        """Main consumer loop that runs in a separate thread"""
        pass

    def poll(self, timeout_ms: int = 1000) -> Dict[str, List[TimeplusRecord]]:
        """Poll for new messages"""
        if not self._running and not self._closed:
            # Start consumer thread if not running
            self._consumer_thread = threading.Thread(target=self._consumer_loop)
            self._consumer_thread.daemon = True
            self._consumer_thread.start()
            self._logger.debug("consumer thread started in poll")

        records = {}
        timeout_sec = timeout_ms / 1000.0
        end_time = time.time() + timeout_sec

        while time.time() < end_time:
            try:
                remaining_time = end_time - time.time()
                if remaining_time <= 0:
                    break

                record = self._message_queue.get(timeout=remaining_time)

                if record.topic not in records:
                    records[record.topic] = []
                records[record.topic].append(record)

                # Continue polling until timeout or queue is empty
                try:
                    while True:
                        record = self._message_queue.get_nowait()
                        if record.topic not in records:
                            records[record.topic] = []
                        records[record.topic].append(record)
                except Empty:
                    break

            except Empty:
                break

        return records

    def pause(self):
        """Pause consumption"""
        with self._pause_condition:
            self._paused = True
            self._logger.info("Consumer paused")

    def resume(self):
        """Resume consumption after being paused"""
        with self._pause_condition:
            self._paused = False
            self._pause_condition.notify_all()
            self._logger.info("Consumer resumed")

    def close(self):
        """Close the consumer"""
        self._closed = True

        # Notify any paused threads to exit
        with self._pause_condition:
            self._pause_condition.notify_all()

        # Wait for consumer thread to finish
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=5.0)

        self._logger.info("Consumer closed")


class SingleTopicConsumer(TimeplusConsumer):
    """Consumer for Timeplus streams that can only consume from a single topic"""

    def __init__(
        self,
        topic: str,
        host: str = "localhost",
        port: int = 8463,
        user: str = "default",
        password: str = "",
        database: str = "default",
        group_id: Optional[str] = None,
        auto_offset_reset: str = "latest",
        partition: int | None = None,
        **kwargs,
    ):
        """Initialize the Timeplus consumer with a specific topic"""
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            partition=partition,
            **kwargs,
        )

        if not topic:
            raise ConsumerException("Topic must be specified for SingleTopicConsumer")

        self._topic = topic

    def get_topic(self) -> str:
        """Get the assigned topic"""
        return self._topic

    def _consumer_loop(self):
        """Main consumer loop that runs in a separate thread"""
        self._running = True

        # Start consumption thread for the topic
        thread = threading.Thread(target=self._consume_stream, args=(self._topic,))
        thread.daemon = True
        thread.start()
        self._topic_threads[self._topic] = thread

        # Wait for thread to complete
        thread.join()

        self._running = False

    def __iter__(self):
        """Make the consumer itself iterable for direct consumption"""
        # Start consumer if not running
        if not self._running and not self._closed:
            self._consumer_thread = threading.Thread(target=self._consumer_loop)
            self._consumer_thread.daemon = True
            self._consumer_thread.start()
            self._logger.debug("note stread started in __iter__")

        while True:
            records = self.poll()
            for _, topic_records in records.items():
                for record in topic_records:
                    yield record


class MultiTopicConsumer(TimeplusConsumer):
    """Consumer for Timeplus streams that can consume from multiple topics"""

    def __init__(
        self,
        topics_or_topic: Union[str, List[str], None] = None,
        host: str = "localhost",
        port: int = 8463,
        user: str = "default",
        password: str = "",
        database: str = "default",
        group_id: Optional[str] = None,
        auto_offset_reset: str = "latest",
        partition: int | None = None,
        **kwargs,
    ):
        """Initialize the Timeplus consumer"""
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            partition=partition,
            **kwargs,
        )

        # Consumer state specific to multi-topic
        self._topics = set()

        self._topic_callbacks = {}

        # Handle initial topic(s) subscription
        if topics_or_topic:
            self.subscribe(topics_or_topic)

    def subscribe(
        self,
        topics: Union[str, List[str]],
        callback: Optional[Callable[[TimeplusRecord], Any]] = None,
    ):
        """Subscribe to topic(s)"""
        # Handle both string and list inputs
        if isinstance(topics, str):
            topics = [topics]

        # Update subscribed topics
        self._topics.update(topics)
        self._logger.info(f"Subscribed to topics: {topics}")

        # Register callbacks if provided
        if callback is not None:
            for topic in topics:
                self._topic_callbacks[topic] = callback

        # Restart consumer if already running
        if self._running:
            self._restart_consumer()

        # Start consumer if not running
        if not self._running and not self._closed:
            self._consumer_thread = threading.Thread(target=self._consumer_loop)
            self._consumer_thread.daemon = True
            self._consumer_thread.start()

    def unsubscribe(self, topics: Union[str, List[str]] = None):
        """Unsubscribe from specified topics, or all topics if none specified"""
        if topics is None:
            # Unsubscribe from all topics
            self._topics.clear()
            self._logger.info("Unsubscribed from all topics")
        else:
            # Unsubscribe from specific topics
            if isinstance(topics, str):
                topics = [topics]

            for topic in topics:
                if topic in self._topics:
                    self._topics.remove(topic)
                    if topic in self._topic_callbacks:
                        del self._topic_callbacks[topic]

            self._logger.info(f"Unsubscribed from topics: {topics}")

        # Restart consumer if already running
        if self._running:
            self._restart_consumer()

    def _restart_consumer(self):
        """Restart the consumer thread with updated topics"""
        self._logger.debug("Restarting consumer with updated topic list")

        # Stop existing consumer
        self._closed = True
        with self._pause_condition:
            self._pause_condition.notify_all()

        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=2.0)

        # Reset state for new consumer
        self._closed = False
        self._running = False
        self._topic_threads.clear()

        # Start new consumer if we have topics
        if self._topics:
            self._consumer_thread = threading.Thread(target=self._consumer_loop)
            self._consumer_thread.daemon = True
            self._consumer_thread.start()

    def _consumer_loop(self):
        """Main consumer loop that runs in a separate thread"""
        self._running = True

        # Start consumption threads for each topic
        for topic in self._topics:
            if (
                topic not in self._topic_threads
                or not self._topic_threads[topic].is_alive()
            ):
                thread = threading.Thread(target=self._consume_stream, args=(topic,))
                thread.daemon = True
                thread.start()
                self._topic_threads[topic] = thread
                self._logger.debug(f"Started consumer thread for topic: {topic}")

        # Wait for all threads to complete
        for topic, thread in self._topic_threads.items():
            thread.join()
            self._logger.debug(f"Consumer thread for topic {topic} has completed")

        self._running = False

    def assignment(self) -> List[str]:
        """Get the list of topics currently assigned to this consumer"""
        return list(self._topics)

    def list_topics(self) -> List[str]:
        """Get list of all available topics/streams"""
        try:
            result = self.client.execute("SHOW STREAMS")
            return [row[0] for row in result]
        except Exception as e:
            self._logger.error(f"Error listing topics: {e}")
            return []
