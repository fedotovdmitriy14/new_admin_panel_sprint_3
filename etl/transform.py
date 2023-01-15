import logging
from typing import Iterator, Type

from psycopg2.extras import DictRow
from pydantic import BaseModel

from state import RedisState

logger = logging.getLogger(__name__)


class DataTransformer:
    def __init__(self, redis_storage: RedisState):
        self.redis_storage = redis_storage

    def check_data_state(
            self,
            data: list[DictRow],
            model: Type[BaseModel],
            index_name: str,
    ) -> Iterator[dict]:
        """
        Проверяется дата последнего обновления модели. Для фильмов - также связанных с ним жанров и персон.
        Если даты в Redis нет или она меньше modified (GREATEST(fw.modified, MAX(p.modified), MAX(g.modified)) - для
        фильмов), то эта запись попадает в генератор, откуда будет сохранен в Elasticsearch
        Также каждая запись валидируется при помощи pydantic
        """
        for row in data:
            row_as_dict = dict(row)
            row_id = row_as_dict.get('id')
            if index_name == 'movies':
                greatest_modified = row_as_dict.get('greatest_modified')
            else:
                greatest_modified = row_as_dict.get('modified')

            try:
                validated_row = model(**row_as_dict)
            except ValueError as e:
                logger.error(e)
                continue

            validated_row_as_dict = validated_row.dict()
            validated_row_as_dict['_id'] = validated_row_as_dict.get('id')
            row_last_modified = self.redis_storage.get_key(row_id)

            if row_last_modified is None or row_last_modified < greatest_modified:
                self.redis_storage.set_key(row_id, greatest_modified)
                yield validated_row_as_dict
