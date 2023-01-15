from datetime import datetime
from typing import Callable

import backoff
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor, DictRow

from settings import PostgresDsl, BATCH_SIZE, backoff_config
from state import State


class PostgresExtractor:
    def __init__(self, dsl: PostgresDsl, redis_storage: State):
        self.dsl = dsl
        self.connection = None
        self.redis_storage = redis_storage

    def check_connection_exists(self) -> None:
        """Создается новое соединение, если его нет или оно закрыто"""
        if self.connection is None or self.connection.closed:
            self.connection = self.create_connection()

    @backoff.on_exception(backoff.expo, Exception, max_tries=backoff_config.backoff_max_tries)
    def create_connection(self) -> connection:
        """Создается новое соединение"""
        self.connection = psycopg2.connect(**self.dsl.dict(), cursor_factory=DictCursor)
        return self.connection

    @backoff.on_exception(backoff.expo, Exception, max_tries=backoff_config.backoff_max_tries)
    def extract_data_from_db(self, index_name: str, query_function: Callable) -> list[DictRow]:
        """
        Делается запрос в базу на получение фильмов и связанных с ними сущностей,
        где поле modified > modified, которое хранится в redis (modified последней записи из запроса)
        Как только запрос не вернул результатов в redis попадает datetime.min, что позволяет циклу в etl пройтись по
        фильмам заново
        """
        self.check_connection_exists()
        last_modified = self.redis_storage.get_key(f'{index_name}_last_modified')
        if last_modified is None:
            self.redis_storage.set_key(f'{index_name}_last_modified', datetime.min)
            last_modified = datetime.min

        query, last_modified_index = query_function(BATCH_SIZE, last_modified)

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
            try:
                last_modified = data[-1][last_modified_index]
            except IndexError:
                last_modified = datetime.min
            self.redis_storage.set_key(f'{index_name}_last_modified', last_modified)

        return data
