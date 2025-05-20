import time
from timeplus_messaging import create_producer, MultiTopicConsumer

# Example usage
if __name__ == "__main__":
    # Producer example
    producer = create_producer(host="localhost", port=8463, user="proton", password="timeplus@t+")
    
    # Send a message
    future = producer.send(
        "test_topic",
        value={"message": "Hello, Timeplus!", "count": 1},
        key="test_key",
        headers={"source": "example"}
    )
    result = future.result()
    print(f"Message sent: {result}")
    
    producer.close()
    
    # Consumer example
    consumer = MultiTopicConsumer(
        host="localhost", 
        port=8463, 
        group_id="test_group", 
        user="proton", 
        password="timeplus@t+",
        auto_offset_reset="earliest")
    
    def on_event(record):
        print(f"Consumed: topic={record.topic}, key={record.key}, "
              f"value={record.value}, timestamp={record.timestamp}")    
    
    consumer.subscribe("test_topic", on_event)
    
    time.sleep(10)
    
    consumer.close()    