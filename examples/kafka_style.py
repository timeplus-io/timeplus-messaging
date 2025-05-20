from timeplus_messaging.producer import TimeplusLogProducer
from timeplus_messaging.consumer import SingleTopicConsumer

# Example usage
if __name__ == "__main__":
    # Producer example
    producer = TimeplusLogProducer(host="localhost", port=8463, user="proton", password="timeplus@t+")
    
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
    consumer = SingleTopicConsumer("test_topic",
        host="localhost", 
        port=8463, 
        group_id="test_group", 
        user="proton", 
        password="timeplus@t+",
        auto_offset_reset="earliest")
    
    for message in consumer:
        print(f"Consumed: topic={message.topic}, key={message.key}, "
            f"value={message.value}, timestamp={message.timestamp} , offset = {message.offset}")