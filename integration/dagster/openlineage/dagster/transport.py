import logging

from openlineage.client.run import RunEvent
from openlineage.client.serde import Serde
from openlineage.client.transport.transport import Transport, Config

log = logging.getLogger(__name__)


class KafkaConfig(Config):
    def __init__(self, topic, producer_config, flush=True):
        self.topic = topic
        self.producer_config = producer_config
        self.flush = flush

    @classmethod
    def from_dict(cls, params: dict):
        return cls(**params)


# Very basic transport impl
class KafkaTransport(Transport):
    kind = "kafka"
    def __init__(self, config: KafkaConfig):
        from kafka import KafkaProducer
        self.config = config
        self.producer = KafkaProducer(**config.producer_config)
        log.debug(f"Constructing openlineage client to send events to topic {config.topic}")

    @staticmethod
    def on_send_error(exc):
        log.error("Error producing kafka message", exc_info=exc)
        raise Exception(str(exc))

    def emit(self, event: RunEvent):
        self.producer.send(topic=self.config.topic, value=Serde.to_json(event).encode('utf-8')).add_errback(self.on_send_error)
        if self.config.flush:
            self.producer.flush()


