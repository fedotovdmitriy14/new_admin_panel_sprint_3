import logging
from typing import Iterator

from psycopg2.extras import DictRow

from models import FilmWork
from state import RedisState

logger = logging.getLogger(__name__)


class DataTransformer:
    def __init__(self, redis_storage: RedisState):
        self.redis_storage = redis_storage

    def check_data_state(self, data: list[DictRow]) -> Iterator[dict]:
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
