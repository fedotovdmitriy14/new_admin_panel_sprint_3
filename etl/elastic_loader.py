import logging
from typing import Iterator

import backoff
from elasticsearch import Elasticsearch, helpers

from models import FilmWork
from settings import BACKOFF_MAX_TRIES, ELASTIC_CONFIG
from state import RedisState


logger = logging.getLogger(__name__)


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
    def create_connection(self) -> Elasticsearch:
        """Создается новое соединение к Redis"""
        self.elastic_connection = Elasticsearch(f"http://{ELASTIC_CONFIG.host}:{ELASTIC_CONFIG.port}")
        return self.elastic_connection

    def check_film_state(self, data: list) -> Iterator[dict]:
        """
        Проверяется дата последнего обновления фильма и связанных с ним жанров и персон.
        Если даты в Redis нет или она меньше GREATEST(fw.modified, MAX(p.modified), MAX(g.modified)),
        то этот фильм попадает в генератор, откуда будет сохранен в Elasticsearch
        Также каждый фильм валидируется при помощи pydantic
        """
        for film in data:
            film_as_dict = dict(film)
            film_id = film_as_dict['id']
            greatest_modified = film_as_dict['greatest_modified']

            try:
                validated_film = FilmWork(**film_as_dict)
            except ValueError as e:
                logger.error(e)
                continue

            validated_film_as_dict = validated_film.dict()
            validated_film_as_dict['_id'] = validated_film_as_dict.get('id')
            film_last_modified = self.redis_storage.get_key(film_id)

            if film_last_modified is None or film_last_modified < greatest_modified:
                self.redis_storage.set_key(film_id, greatest_modified)
                yield validated_film_as_dict

    @backoff.on_exception(backoff.expo, Exception, max_tries=BACKOFF_MAX_TRIES)
    def update_elasticsearch(self, data: list) -> None:
        self.create_connection()
        films_to_insert = self.check_film_state(data)

        try:
            response = helpers.bulk(
                client=self.elastic_connection,
                actions=films_to_insert,
                index='movies',
                chunk_size=500,
            )
            logger.info('\nRESPONSE:', response)

        except Exception as e:
            logger.info('\nERROR:', e)
