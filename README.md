# Timeplus Messaging

A Python package for messaging with Timeplus streams. This library provides a simplified messaging interface on top of Timeplus, supporting message publishing and consumption patterns similar to traditional messaging systems.

## Features

- **Producer API**: Easily send messages to Timeplus streams
- **Consumer API**: Consume messages from one or multiple Timeplus streams
- **Topic-based messaging**: Use Timeplus streams as message topics
- **Message headers support**: Include metadata with your messages
- **Batch consumption**: Efficiently poll for multiple messages
- **Pause/resume capability**: Control consumption flow
- **Stream auto-creation**: Automatically create streams as needed
- **Callback-based processing**: Register callbacks for specific topics

## Installation

```bash
pip install timeplus-messaging
```

## Requirements

- Python 3.7 or higher
- proton-driver (Timeplus client driver)

## Quick Start

### Producer Example

```python
from timeplus_messaging import create_producer

# Create a producer
producer = create_producer(
    host="your-timeplus-host",
    port=8463,
    user="default",
    password="your-password",
    database="default"
)

# Send a simple message
producer.send(
    topic="my_topic",
    value="Hello, Timeplus!",
    key="greeting"
)

# Send a JSON message
producer.send(
    topic="my_topic",
    value={"message": "Hello, Timeplus!", "priority": "high"},
    headers={"source": "application", "version": "1.0"}
)

# Close the producer
producer.close()
```

### Single Topic Consumer Example - Kafka Style

```python
from timeplus_messaging import create_consumer

# Create a single topic consumer
consumer = create_consumer(
    topic="my_topic",
    host="your-timeplus-host",
    port=8463,
    user="default",
    password="your-password",
    database="default",
    auto_offset_reset="latest"  # or "earliest"
)

# Poll for messages
records = consumer.poll(timeout_ms=5000)
for topic, messages in records.items():
    for message in messages:
        print(f"Received: {message.value} with key={message.key}")

# Use as iterator
for record in consumer:
    print(f"Received: {record.value}")
    # Break out of the loop after processing
    break

# Close the consumer
consumer.close()
```

### Multi-Topic Consumer Example - Queue Style

```python
from timeplus_messaging import create_subscribe_consumer

# Create a multi-topic consumer
consumer = create_subscribe_consumer(
    host="your-timeplus-host",
    port=8463,
    user="default",
    password="your-password",
    database="default",
    auto_offset_reset="latest"
)


# Add a callback for a specific topic
def process_message(record):
    print(f"Callback received: {record.value}")

# Subscribe to multiple topics
consumer.subscribe(["topic1", "topic2"], callback=process_message)

# wait all message to be consumed
time.sleep(10) 

# Unsubscribe from topic
consumer.unsubscribe("topic2")

# Close the consumer
consumer.close()
```

## Advanced Usage

### Using Consumer Callbacks

```python
from timeplus_messaging import create_subscribe_consumer

consumer = create_subscribe_consumer(
    host="your-timeplus-host",
    port=8463,
    user="default",
    password="your-password"
)

def process_logs(record):
    print(f"Log received: {record.value}")
    # Process log message
    
def process_metrics(record):
    print(f"Metric received: {record.value}")
    # Process metric data

# Subscribe with callbacks
consumer.subscribe("logs", callback=process_logs)
consumer.subscribe("metrics", callback=process_metrics)
```



## License

MIT