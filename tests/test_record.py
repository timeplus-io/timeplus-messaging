"""
Tests for the Timeplus record classes
"""

import unittest
import time
from timeplus_messaging.record import TimeplusRecord, ProducerRecord


class TestTimeplusRecord(unittest.TestCase):
    """Test cases for the TimeplusRecord class"""
    
    def test_timeplus_record_initialization(self):
        """Test basic initialization of TimeplusRecord"""
        record = TimeplusRecord(
            topic="test_topic",
            value="test_value",
            key="test_key",
            partition=1,
            offset=100,
            timestamp=1234567890000,
            headers={"source": "test"}
        )
        
        self.assertEqual(record.topic, "test_topic")
        self.assertEqual(record.value, "test_value")
        self.assertEqual(record.key, "test_key")
        self.assertEqual(record.partition, 1)
        self.assertEqual(record.offset, 100)
        self.assertEqual(record.timestamp, 1234567890000)
        self.assertEqual(record.headers, {"source": "test"})
    
    def test_timeplus_record_default_values(self):
        """Test default values for TimeplusRecord"""
        record = TimeplusRecord(
            topic="test_topic",
            value="test_value"
        )
        
        self.assertEqual(record.topic, "test_topic")
        self.assertEqual(record.value, "test_value")
        self.assertIsNone(record.key)
        self.assertEqual(record.partition, 0)
        self.assertIsNone(record.offset)
        self.assertIsNotNone(record.timestamp)
        self.assertIsNone(record.headers)
    
    def test_timeplus_record_auto_timestamp(self):
        """Test that timestamp is auto-generated if not provided"""
        # Get current time
        current_time = int(time.time() * 1000)
        
        # Create record without timestamp
        record = TimeplusRecord(
            topic="test_topic",
            value="test_value"
        )
        
        # Verify timestamp is close to current time (within 100ms)
        self.assertIsNotNone(record.timestamp)
        self.assertTrue(abs(record.timestamp - current_time) < 100)


class TestProducerRecord(unittest.TestCase):
    """Test cases for the ProducerRecord class"""
    
    def test_producer_record_initialization(self):
        """Test basic initialization of ProducerRecord"""
        record = ProducerRecord(
            topic="test_topic",
            value="test_value",
            key="test_key",
            partition=1,
            timestamp=1234567890000,
            headers={"source": "test"}
        )
        
        self.assertEqual(record.topic, "test_topic")
        self.assertEqual(record.value, "test_value")
        self.assertEqual(record.key, "test_key")
        self.assertEqual(record.partition, 1)
        self.assertEqual(record.timestamp, 1234567890000)
        self.assertEqual(record.headers, {"source": "test"})
    
    def test_producer_record_default_values(self):
        """Test default values for ProducerRecord"""
        record = ProducerRecord(
            topic="test_topic",
            value="test_value"
        )
        
        self.assertEqual(record.topic, "test_topic")
        self.assertEqual(record.value, "test_value")
        self.assertIsNone(record.key)
        self.assertIsNone(record.partition)
        self.assertIsNone(record.timestamp)
        self.assertIsNone(record.headers)


if __name__ == '__main__':
    unittest.main()