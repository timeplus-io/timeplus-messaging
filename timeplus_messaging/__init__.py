from timeplus_messaging.producer import TimeplusProducer, TimeplusLogProducer
from timeplus_messaging.consumer import SingleTopicConsumer, MultiTopicConsumer
from timeplus_messaging.admin import TimeplusAdmin


def create_producer(**config) -> TimeplusProducer:
    """Create a Timeplus producer with the given configuration"""
    return TimeplusLogProducer(**config)


def create_consumer(topic, **config) -> SingleTopicConsumer:
    """Create a Timeplus consumer with the given configuration"""
    return SingleTopicConsumer(topic, **config)


def create_subscribe_consumer(**config) -> MultiTopicConsumer:
    """Create a Timeplus consumer with the given configuration"""
    return MultiTopicConsumer(**config)


def create_admin(**config) -> TimeplusAdmin:
    """Create a Timeplus admin with the given configuration"""
    return TimeplusAdmin(**config)
