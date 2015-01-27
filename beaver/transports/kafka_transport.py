# -*- coding: utf-8 -*-
from kafka import KafkaClient, SimpleProducer

from beaver.transports.base_transport import BaseTransport
from beaver.transports.exception import TransportException


class KafkaTransport(BaseTransport):
    """Transport layer for Kafka, High-level API"""

    DELIVERY_MODE = {
        'ack_after_local_write' : SimpleProducer.ACK_AFTER_LOCAL_WRITE,
        'ack_after_cluster_commit' : SimpleProducer.ACK_AFTER_CLUSTER_COMMIT,
    }

    def __init__(self, beaver_config, logger=None):
        super(KafkaTransport, self).__init__(beaver_config, logger=logger)

        self._kafka_config = {}
        config_to_store = [
            'host', 'port', 'delivery_mode', 'ack_timeout', 'async', 'topic', 
            'batch_send', 'batch_send_every_n', 'batch_send_every_t'
        ]

        for key in config_to_store:
            self._kafka_config[key] = beaver_config.get('kafka_' + key)

        self._logger = logger
        self._client = None
        self._producer = None
        self._topic = self._kafka_config['topic'] # set a default topic ?
        if not self._topic:
            raise Exception('Invalid Kafka Topic Setting')
        self._connect()

    def _connect(self):
        self._is_valid = False
        if self._client:
            self._client.close()
        # Setup Kafka producer
        self._client = KafkaClient("{host}:{port}".format(host=self._kafka_config['host'], port=self._kafka_config['port']))
        self._producer = SimpleProducer(self._client, async=self._kafka_config.get('async', False),
                                        req_acks=self.DELIVERY_MODE.get(self._kafka_config['delivery_mode'], SimpleProducer.ACK_AFTER_LOCAL_WRITE),
                                        ack_timeout=self._kafka_config.get('ack_timeout', 2000),
                                        batch_send=self._kafka_config.get('batch_send', True),
                                        batch_send_every_n=self._kafka_config.get('batch_send_every_n', 60),
                                        batch_send_every_t=self._kafka_config.get('batch_send_every_t', 60)
                                        )
        self._logger.info('Connect to Kafka({host}:{port}).'.format(host=self._kafka_config['host'], port=self._kafka_config['port']))
        self._is_valid = True

    def callback(self, filename, lines, **kwargs):
        timestamp = self.get_timestamp(**kwargs)
        if kwargs.get('timestamp', False):
            del kwargs['timestamp']

        for line in lines:
            try:
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter('error')
                    response = self._producer.send_messages(self._topic, self.format(filename, line, timestamp, **kwargs))
                    if response:
                        self._logger.debug('Kafka send message response : {response}'.format(response=response))
                    else:
                        raise
            except UserWarning:
                self._is_valid = False
                raise TransportException('Connection appears to have been lost')
            except Exception as e:
                self._is_valid = False
                try:
                    raise TransportException(e.strerror)
                except AttributeError:
                    raise TransportException('Unspecified exception encountered')  # TRAP ALL THE THINGS!

    def interrupt(self):
        if self._client:
            self._client.close()
            self._client = None

    def reconnect(self):
        self._connect()

    def unhandled(self):
        return True
