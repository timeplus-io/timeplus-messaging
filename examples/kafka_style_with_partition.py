from timeplus_messaging.producer import TimeplusLogProducer
from timeplus_messaging.consumer import SingleTopicConsumer
from timeplus_messaging.admin import TimeplusAdmin, AdminException

# Example usage
if __name__ == "__main__":
    
    admin = TimeplusAdmin(host="localhost", port=8463, user="proton", password="timeplus@t+")
    try:
        admin.delete_topic("test_topic")
    except AdminException as e:
        pass
    
    admin.create_topic("test_topic", partitions=3)
    
    # Producer example
    producer = TimeplusLogProducer(host="localhost", port=8463, user="proton", password="timeplus@t+")
    
    # Send 10 messages
    for i in range(10):
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
        auto_offset_reset="earliest",
        partition=1
    )
    
    for message in consumer:
        print(f"Consumed: topic={message.topic}, key={message.key}, "
            f"value={message.value}, timestamp={message.timestamp} , offset = {message.offset}, partition = {message.partition}")