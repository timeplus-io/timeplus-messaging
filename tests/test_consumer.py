"""
Tests for the Timeplus consumer implementations
"""

import unittest
import json
import datetime
from queue import Queue
from unittest.mock import MagicMock, patch, call

from timeplus_messaging import create_consumer, create_subscribe_consumer
from timeplus_messaging.consumer import SingleTopicConsumer, MultiTopicConsumer, ConsumerException


class TestSingleTopicConsumer(unittest.TestCase):
    """Test cases for the SingleTopicConsumer"""

    @patch('timeplus_messaging.consumer.client.Client')
    def setUp(self, mock_client):
        """Set up the test case"""
        self.mock_client_instance = MagicMock()
        mock_client.return_value = self.mock_client_instance
        
        self.consumer = create_consumer(
            topic="test_topic",
            host="localhost", 
            port=8463, 
            user="default", 
            database="default",
            auto_offset_reset="latest"
        )
    
    def test_consumer_initialization(self):
        """Test that consumer initializes correctly"""
        self.assertIsInstance(self.consumer, SingleTopicConsumer)
        self.assertEqual(self.consumer.host, "localhost")
        self.assertEqual(self.consumer.port, 8463)
        self.assertEqual(self.consumer.user, "default")
        self.assertEqual(self.consumer.database, "default")
        self.assertEqual(self.consumer.auto_offset_reset, "latest")
        self.assertEqual(self.consumer.get_topic(), "test_topic")
        
        # Verify unique group_id format
        self.assertTrue(self.consumer.group_id.startswith("timeplus_consumer_"))
        self.assertEqual(len(self.consumer.group_id) - len("timeplus_consumer_"), 8)

    
    @patch('timeplus_messaging.consumer.client.Client')
    def test_auto_offset_reset_earliest(self, mock_client):
        """Test auto_offset_reset=earliest generates correct query"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        # Mock execute_iter to return empty results
        mock_client_instance.execute_iter.return_value = []
        
        consumer = create_consumer(
            topic="test_topic",
            host="localhost",
            auto_offset_reset="earliest"
        )
        
        # Start consuming
        consumer._consume_stream("test_topic")
        
        # Check that the correct query was executed
        expected_query = "SELECT _tp_time, _key, _value, _headers, _tp_sn, _tp_shard FROM test_topic WHERE _tp_time >= earliest_ts()"
        mock_client_instance.execute_iter.assert_called_with(expected_query)
    
    @patch('timeplus_messaging.consumer.client.Client')
    def test_auto_offset_reset_latest(self, mock_client):
        """Test auto_offset_reset=latest generates correct query"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        # Mock execute_iter to return empty results
        mock_client_instance.execute_iter.return_value = []
        
        consumer = create_consumer(
            topic="test_topic",
            host="localhost",
            auto_offset_reset="latest"
        )
        
        # Start consuming
        consumer._consume_stream("test_topic")
        
        # Check that the correct query was executed
        expected_query = "SELECT _tp_time, _key, _value, _headers, _tp_sn, _tp_shard FROM test_topic WHERE _tp_time >= now()"
        mock_client_instance.execute_iter.assert_called_with(expected_query)
    
    @patch('timeplus_messaging.consumer.client.Client')
    def test_consumer_poll_with_messages(self, mock_client):
        """Test polling for messages"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        # Create a consumer with a mocked queue
        consumer = create_consumer(topic="test_topic", host="localhost")
        
        # Add some mock messages to the queue
        now = datetime.datetime.now()
        record1 = MagicMock(topic="test_topic", key="key1", value="value1", offset=1)
        record2 = MagicMock(topic="test_topic", key="key2", value="value2", offset=2)
        
        consumer._message_queue = Queue()
        consumer._message_queue.put(record1)
        consumer._message_queue.put(record2)
        
        # Poll for messages
        records = consumer.poll(timeout_ms=100)
        
        # Verify records were returned correctly
        self.assertEqual(len(records), 1)  # One topic
        self.assertEqual(len(records["test_topic"]), 2)  # Two messages
        self.assertEqual(records["test_topic"][0], record1)
        self.assertEqual(records["test_topic"][1], record2)
    
    def test_consumer_pause_resume(self):
        """Test pausing and resuming consumer"""
        # Initially not paused
        self.assertFalse(self.consumer._paused)
        
        # Pause
        self.consumer.pause()
        self.assertTrue(self.consumer._paused)
        
        # Resume
        self.consumer.resume()
        self.assertFalse(self.consumer._paused)
    
    @patch('timeplus_messaging.consumer.threading.Thread')
    def test_consumer_close(self, mock_thread):
        """Test closing consumer"""
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance
        
        # Set up the consumer thread
        self.consumer._consumer_thread = mock_thread_instance
        
        # Close consumer
        self.consumer.close()
        
        # Verify state
        self.assertTrue(self.consumer._closed)
        
        # Verify thread was joined
        mock_thread_instance.join.assert_called_once()


class TestMultiTopicConsumer(unittest.TestCase):
    """Test cases for the MultiTopicConsumer"""

    @patch('timeplus_messaging.consumer.client.Client')
    def setUp(self, mock_client):
        """Set up the test case"""
        self.mock_client_instance = MagicMock()
        mock_client.return_value = self.mock_client_instance
        
        self.consumer = create_subscribe_consumer(
            host="localhost", 
            port=8463, 
            user="default", 
            database="default",
            auto_offset_reset="latest"
        )
    
    def test_consumer_initialization(self):
        """Test that consumer initializes correctly"""
        self.assertIsInstance(self.consumer, MultiTopicConsumer)
        self.assertEqual(self.consumer.host, "localhost")
        self.assertEqual(self.consumer.port, 8463)
        self.assertEqual(self.consumer.user, "default")
        self.assertEqual(self.consumer.database, "default")
        
        # Initially no topics
        self.assertEqual(len(self.consumer._topics), 0)
    
    @patch('timeplus_messaging.consumer.client.Client')
    def test_initialization_with_topics(self, mock_client):
        """Test that consumer can be initialized with topics"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        consumer = create_subscribe_consumer(
            topics_or_topic=["topic1", "topic2"],
            host="localhost"
        )
        
        self.assertEqual(len(consumer._topics), 2)
        self.assertIn("topic1", consumer._topics)
        self.assertIn("topic2", consumer._topics)
    
    @patch('timeplus_messaging.consumer.client.Client')
    def test_initialization_with_single_topic(self, mock_client):
        """Test that consumer can be initialized with a single topic string"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        consumer = create_subscribe_consumer(
            topics_or_topic="test_topic",
            host="localhost"
        )
        
        self.assertEqual(len(consumer._topics), 1)
        self.assertIn("test_topic", consumer._topics)
    
    def test_subscribe_to_topics(self):
        """Test subscribing to topics"""
        # Subscribe to topics
        self.consumer.subscribe(["topic1", "topic2"])
        
        # Verify topics were added
        self.assertEqual(len(self.consumer._topics), 2)
        self.assertIn("topic1", self.consumer._topics)
        self.assertIn("topic2", self.consumer._topics)
        
        # Subscribe to another topic
        self.consumer.subscribe("topic3")
        
        # Verify topic was added
        self.assertEqual(len(self.consumer._topics), 3)
        self.assertIn("topic3", self.consumer._topics)
    
    def test_subscribe_with_callback(self):
        """Test subscribing with callbacks"""
        # Define a callback function
        callback_fn = lambda x: x
        
        # Subscribe with callback
        self.consumer.subscribe("topic1", callback=callback_fn)
        
        # Verify callback was registered
        self.assertEqual(self.consumer._topic_callbacks["topic1"], callback_fn)
    
    def test_unsubscribe_from_topics(self):
        """Test unsubscribing from topics"""
        # First subscribe to some topics
        self.consumer.subscribe(["topic1", "topic2", "topic3"])
        
        # Unsubscribe from specific topic
        self.consumer.unsubscribe("topic2")
        
        # Verify topic was removed
        self.assertEqual(len(self.consumer._topics), 2)
        self.assertIn("topic1", self.consumer._topics)
        self.assertIn("topic3", self.consumer._topics)
        self.assertNotIn("topic2", self.consumer._topics)
        
        # Unsubscribe from all topics
        self.consumer.unsubscribe()
        
        # Verify all topics were removed
        self.assertEqual(len(self.consumer._topics), 0)
    
    def test_assignment(self):
        """Test getting topic assignments"""
        # Subscribe to topics
        self.consumer.subscribe(["topic1", "topic2"])
        
        # Get assignments
        assignments = self.consumer.assignment()
        
        # Verify assignments match subscribed topics
        self.assertEqual(len(assignments), 2)
        self.assertIn("topic1", assignments)
        self.assertIn("topic2", assignments)
    
    def test_list_topics(self):
        """Test listing available topics"""
        # Mock the execute result
        self.mock_client_instance.execute.return_value = [
            ("topic1",),
            ("topic2",),
            ("topic3",)
        ]
        
        # List topics
        topics = self.consumer.list_topics()
        
        # Verify topics
        self.assertEqual(len(topics), 3)
        self.assertEqual(topics, ["topic1", "topic2", "topic3"])
        
        # Verify correct query was executed
        self.mock_client_instance.execute.assert_called_with("SHOW STREAMS")


if __name__ == '__main__':
    unittest.main()