import json
import time
import logging
from abc import abstractmethod

from typing import Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, Future

# Import proton driver
from proton_driver import client, connect
from proton_driver.errors import Error as ProtonError

from timeplus_messaging.record import ProducerRecord, TimeplusRecord

class ProducerException(Exception):
    """Exception raised by the producer"""
    pass


class TimeplusProducer:
    """
    Producer for Timeplus based on append only streams
    """
    
    def __init__(self, 
                 host: str = "localhost",
                 port: int = 8463,
                 user: str = "default",
                 password: str = "",
                 database: str = "default",
                 **kwargs):
        """
        Initialize the Timeplus producer
        
        Args:
            host: Timeplus server host
            port: Timeplus server port
            user: Username for authentication
            password: Password for authentication
            database: Database name
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        
        # Connection settings
        self.client = client.Client(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            **kwargs
        )
        
        # Producer state
        self._closed = False
        self._executor = ThreadPoolExecutor(max_workers=10)
        self._logger = logging.getLogger(self.__class__.__name__)
        
        # Stream schemas cache
        self._stream_schemas: Dict[str, Dict] = {}
        
    @abstractmethod
    def _ensure_stream_exists(self, topic: str, schema: Optional[Dict] = None):
        pass
    
    def send(self, 
             topic: str, 
             value: Any,
             key: Optional[str] = None,
             partition: Optional[int] = None,
             timestamp: Optional[int] = None,
             headers: Optional[Dict[str, Any]] = None) -> Future:
        """
        Send a record to the specified topic
        
        Args:
            topic: The topic to send to
            value: The message value
            key: Optional message key
            partition: Optional partition (ignored in Timeplus)
            timestamp: Optional timestamp
            headers: Optional headers dictionary
        
        Returns:
            Future object representing the send operation
        """
        if self._closed:
            raise ProducerException("Producer is closed")
        
        record = ProducerRecord(
            topic=topic,
            value=value,
            key=key,
            partition=partition,
            timestamp=timestamp,
            headers=headers
        )
        
        return self._executor.submit(self._send_record, record)
    
    def _send_record(self, record: ProducerRecord) -> Dict:
        """Internal method to send a record"""
        try:
            # Ensure stream exists
            self._ensure_stream_exists(record.topic)
            
            # Prepare the data
            timestamp = record.timestamp or int(time.time() * 1000)
            headers_json = json.dumps(record.headers or {})
            
            # Insert data into stream
            insert_sql = f"""
                INSERT INTO {record.topic} (_tp_time, _key, _value, _headers) 
                VALUES (from_unix_timestamp64_milli({timestamp}), %(key)s, %(value)s, %(headers)s)
            """
            
            # Convert value to string if it's not already
            if isinstance(record.value, (dict, list)):
                value_str = json.dumps(record.value)
            else:
                value_str = str(record.value)
            
            params = {
                'key': record.key or '',
                'value': value_str,
                'headers': headers_json
            }
            
            self.client.execute(insert_sql, params)
            
            return {
                'topic': record.topic,
                'partition': 0,
                'offset': None,  # TODO: return the _tp_sn as offset of the insert
                'timestamp': timestamp
            }
            
        except Exception as e:
            self._logger.error(f"Failed to send record: {e}")
            raise ProducerException(f"Send failed: {e}")
    
    def send_sync(self, *args, **kwargs) -> Dict:
        """Send a record synchronously"""
        future = self.send(*args, **kwargs)
        return future.result()
    
    def flush(self, timeout: Optional[float] = None):
        """Wait for all pending sends to complete"""
        # Since we're using ThreadPoolExecutor, we can wait for completion
        # In a real implementation, you might want to track pending futures
        pass
    
    def close(self):
        """Close the producer"""
        self._closed = True
        self._executor.shutdown(wait=True)    
        

class TimeplusLogProducer(TimeplusProducer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._logger = logging.getLogger(__name__)  
        
    def _ensure_stream_exists(self, topic: str, schema: Optional[Dict] = None):
        """Ensure that a stream exists, create it if it doesn't"""
        try:
            # Check if stream exists
            result = self.client.execute(f"SHOW STREAMS LIKE '{topic}'")
            if not result:
                # Stream doesn't exist, create it
                if schema:
                    columns = []
                    for col_name, col_type in schema.items():
                        columns.append(f"{col_name} {col_type}")
                    columns_def = ", ".join(columns)
                else:
                    # Default schema for generic messaging
                    columns_def = (
                        "_key string DEFAULT '', "
                        "_value string, "
                        "_headers string DEFAULT '{}'"
                    ) # _tp_time and _tp_sn are default internal columns, no need create explicitly
                
                create_sql = f"CREATE STREAM IF NOT EXISTS {topic} ({columns_def})"
                self.client.execute(create_sql)
                self._logger.info(f"Created stream: {topic}")
                
        except ProtonError as e:
            self._logger.error(f"Error ensuring stream exists: {e}")
            raise ProducerException(f"Failed to create stream {topic}: {e}")
        