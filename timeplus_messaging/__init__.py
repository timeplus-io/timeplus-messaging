from timeplus_messaging.producer import TimeplusProducer, TimeplusLogProducer
from timeplus_messaging.consumer import TimeplusConsumer, SingleTopicConsumer, MultiTopicConsumer


def create_producer(**config) -> TimeplusProducer:
    """Create a Timeplus producer with the given configuration"""
    return TimeplusLogProducer(**config)


def create_consumer(topic, **config) -> SingleTopicConsumer:
    """Create a Timeplus consumer with the given configuration"""
    return SingleTopicConsumer(topic, **config)

def create_subscribe_consumer(**config) -> MultiTopicConsumer:
    """Create a Timeplus consumer with the given configuration"""
    return MultiTopicConsumer(**config)


