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
    ) -> Iterator[dict]:
        """
        Проверяется дата последнего обновления фильма и связанных с ним жанров и персон.
        Если даты в Redis нет или она меньше GREATEST(fw.modified, MAX(p.modified), MAX(g.modified)),
        то этот фильм попадает в генератор, откуда будет сохранен в Elasticsearch
        Также каждый фильм валидируется при помощи pydantic
        """
        for row in data:
            row_as_dict = dict(row)
            row_id = row_as_dict['id']
            greatest_modified = row_as_dict['greatest_modified']

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
