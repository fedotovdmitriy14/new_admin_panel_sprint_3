from typing import Iterator

import backoff
from elasticsearch import Elasticsearch

from models import FilmWork
from settings import BACKOFF_MAX_TRIES, elastic_config
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
    def create_connection(self) -> Elasticsearch:
        """Создается новое соединение к Redis"""
        self.elastic_connection = Elasticsearch(f"http://{elastic_config['host']}:{elastic_config['port']}")
        return self.elastic_connection

    def check_film_state(self, data: list) -> Iterator[dict]:
        """
        Проверяется дата последнего обновления фильма и связанных с ним жанров и персон.
        Если даты в Redis нет или она меньше GREATEST(fw.modified, MAX(p.modified), MAX(g.modified)),
        то этот фильм попадает в генератор, откуда будет сохранен в Elasticsearch
        """
        for film in data:
            film_as_dict = dict(film)
            film_id = film_as_dict['id']
            greatest_modified = film_as_dict['greatest_modified']
            film_model = FilmWork(**film_as_dict)
            film_last_modified = self.redis_storage.get_key(film_id)

            if film_last_modified is None or film_last_modified < greatest_modified:
                self.redis_storage.set_key(film_id, greatest_modified)
                yield film_model

    @backoff.on_exception(backoff.expo, Exception, max_tries=BACKOFF_MAX_TRIES)
    def update_elasticsearch(self, data: list):
        films_to_insert = self.check_film_state(data)
        print(films_to_insert)
        if films_to_insert:
            for i in films_to_insert:
                print(i)
