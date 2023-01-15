import logging
from typing import Optional, Type

import backoff
from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictRow
from pydantic import BaseModel

from settings import BATCH_SIZE, ElasticConfig, backoff_config, redis_config
from state import RedisState
from transform import DataTransformer

logger = logging.getLogger(__name__)


class ElasticLoader:
    def __init__(
            self,
            config: ElasticConfig,
            transformer: DataTransformer = DataTransformer(RedisState(redis_config)),
    ):
        self.config = config
        self.transformer = transformer
        self.elastic_connection = None

    def is_connection_alive(self) -> bool:
        return self.elastic_connection.ping()

    def check_connection_exists(self) -> None:
        """Проверяется наличие соединения к ElasticSearch"""
        if self.elastic_connection and self.is_connection_alive():
            return self.elastic_connection
        self.elastic_connection = self.create_connection()

    @backoff.on_exception(backoff.expo, Exception, max_tries=backoff_config.backoff_max_tries)
    def create_connection(self) -> Elasticsearch:
        """Создается новое соединение к Redis"""
        self.elastic_connection = Elasticsearch(f"http://{self.config.host}:{self.config.port}")
        return self.elastic_connection

    @backoff.on_exception(backoff.expo, Exception, max_tries=backoff_config.backoff_max_tries)
    def update_elasticsearch(
            self,
            data: list[DictRow],
            index_model: Optional[Type[BaseModel]],
            index_name: str) -> None:
        """
        data отправляется в check_data_state, который возвращает генератор из записей для update/insert,
        которые пачками отправляются в elasticsearch
        """
        self.create_connection()
        values_to_insert = self.transformer.check_data_state(data, model=index_model, index_name=index_name)

        try:
            response = helpers.bulk(
                client=self.elastic_connection,
                actions=values_to_insert,
                index=index_name,
                chunk_size=BATCH_SIZE,
            )
            logger.info(response)

        except Exception as e:
            logger.info(e)
