"""
Tests for the Timeplus producer implementation
"""

import unittest
import json
import time
from unittest.mock import MagicMock, patch

from timeplus_messaging import create_producer
from timeplus_messaging.producer import TimeplusLogProducer, ProducerException


class TestTimeplusProducer(unittest.TestCase):
    """Test cases for the Timeplus producer"""

    @patch('timeplus_messaging.producer.client.Client')
    def setUp(self, mock_client):
        """Set up the test case"""
        self.mock_client_instance = MagicMock()
        mock_client.return_value = self.mock_client_instance
        
        self.producer = create_producer(
            host="localhost", 
            port=8463, 
            user="default", 
            database="default"
        )
    
    def test_producer_initialization(self):
        """Test that producer initializes correctly"""
        self.assertIsInstance(self.producer, TimeplusLogProducer)
        self.assertEqual(self.producer.host, "localhost")
        self.assertEqual(self.producer.port, 8463)
        self.assertEqual(self.producer.user, "default")
        self.assertEqual(self.producer.database, "default")
    
    @patch('timeplus_messaging.producer.client.Client')
    def test_ensure_stream_exists_check(self, mock_client):
        """Test that _ensure_stream_exists checks if stream exists"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        # Mock the stream exists
        mock_client_instance.execute.return_value = [("test_topic",)]
        
        producer = TimeplusLogProducer(host="localhost")
        producer._ensure_stream_exists("test_topic")
        
        # Verify SHOW STREAMS was called
        mock_client_instance.execute.assert_called_with("SHOW STREAMS LIKE 'test_topic'")
    
    @patch('timeplus_messaging.producer.client.Client')
    def test_ensure_stream_exists_create(self, mock_client):
        """Test that _ensure_stream_exists creates stream if not exists"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        # Mock stream doesn't exist, then create succeeds
        mock_client_instance.execute.side_effect = [
            [],  # No stream found
            None  # Create successful
        ]
        
        producer = TimeplusLogProducer(host="localhost")
        producer._ensure_stream_exists("test_topic")
        
        # Verify CREATE STREAM was called
        expected_sql = (
            "CREATE STREAM IF NOT EXISTS test_topic ("
            "_key string DEFAULT '', "
            "_value string, "
            "_headers string DEFAULT '{}')"
        )
        mock_client_instance.execute.assert_called_with(expected_sql)
    
    @patch('timeplus_messaging.producer.client.Client')
    def test_ensure_stream_exists_with_schema(self, mock_client):
        """Test that _ensure_stream_exists creates stream with custom schema"""
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance
        
        # Mock stream doesn't exist, then create succeeds
        mock_client_instance.execute.side_effect = [
            [],  # No stream found
            None  # Create successful
        ]
        
        schema = {
            "user_id": "string",
            "event_type": "string",
            "timestamp": "datetime64"
        }
        
        producer = TimeplusLogProducer(host="localhost")
        producer._ensure_stream_exists("test_topic", schema)
        
        # Verify CREATE STREAM was called with custom schema
        calls = mock_client_instance.execute.call_args_list
        create_call = calls[1]
        create_sql = create_call[0][0]
        
        self.assertIn("CREATE STREAM IF NOT EXISTS test_topic", create_sql)
        self.assertIn("user_id string", create_sql)
        self.assertIn("event_type string", create_sql)
        self.assertIn("timestamp datetime64", create_sql)
    
    def test_send_method(self):
        """Test sending a message"""
        # Setup mock response
        self.mock_client_instance.execute.return_value = []  # Stream check
        
        # Send a message
        future = self.producer.send(
            topic="test_topic",
            value="test_message",
            key="test_key",
            headers={"source": "test"}
        )
        
        # Get result (blocks until complete)
        result = future.result()
        
        # Verify the insert SQL contains correct values
        calls = self.mock_client_instance.execute.call_args_list
        self.assertEqual(len(calls), 3)  # One for check, one for create, one for insert
        
        insert_call = calls[2]
        insert_sql = insert_call[0][0]
        params = insert_call[0][1]
        
        self.assertIn("INSERT INTO test_topic", insert_sql)
        self.assertEqual(params['key'], "test_key")
        self.assertEqual(params['value'], "test_message")
        self.assertEqual(params['headers'], json.dumps({"source": "test"}))
    
    def test_send_dict_value(self):
        """Test sending a dictionary value"""
        self.mock_client_instance.execute.return_value = []  # Stream check
        
        value = {"message": "hello", "priority": 1}
        
        future = self.producer.send(
            topic="test_topic",
            value=value
        )
        
        result = future.result()
        
        calls = self.mock_client_instance.execute.call_args_list
        insert_call = calls[2]
        params = insert_call[0][1]
        
        self.assertEqual(params['value'], json.dumps(value))
    
    def test_send_sync(self):
        """Test sending a message synchronously"""
        self.mock_client_instance.execute.return_value = []  # Stream check
        
        result = self.producer.send_sync(
            topic="test_topic",
            value="test_message"
        )
        
        self.assertEqual(result['topic'], "test_topic")
        self.assertEqual(result['partition'], 0)
        
        # Should have timestamp
        self.assertIsNotNone(result['timestamp'])
    
    def test_producer_close(self):
        """Test that close method works correctly"""
        self.producer.close()
        self.assertTrue(self.producer._closed)
        
        # Verify sending after close raises exception
        with self.assertRaises(ProducerException):
            self.producer.send("test_topic", "test_message")

    def test_producer_error_handling(self):
        """Test producer error handling"""
        # Mock client to raise exception
        self.mock_client_instance.execute.side_effect = Exception("Test error")
        
        with self.assertRaises(ProducerException) as context:
            self.producer.send_sync("test_topic", "test_message")
        
        self.assertIn("Send failed", str(context.exception))


if __name__ == '__main__':
    unittest.main()