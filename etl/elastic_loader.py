import backoff
from elasticsearch import Elasticsearch

from settings import BACKOFF_MAX_TRIES, elastic_config, redis_config
from state import RedisState


class ElasticLoader:
    def __init__(self, config: dict, redis_storage: RedisState):
        self.config = config
        self.redis_storage = redis_storage
        self.elastic_connection = None

    def is_connection_alive(self) -> bool:
        return self.elastic_connection.ping()

    def check_connection_exists(self) -> None:
        """Проверяется наличие соединения к ElasticSearch"""
        if self.elastic_connection and self.is_connection_alive():
            return self.elastic_connection
        self.elastic_connection = self.create_connection()

    @backoff.on_exception(backoff.expo, Exception, max_tries=BACKOFF_MAX_TRIES)
    def create_connection(self) -> None:
        """Создается новое соединение к Redis"""
        self.elastic_connection = Elasticsearch(f"http://{elastic_config['host']}:{elastic_config['port']}")
        print(self.elastic_connection)
        self.is_connection_alive()
