from datetime import datetime

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
    def extract_data_from_db(self) -> list[DictRow]:
        """
        Делается запрос в базу на получение фильмов и связанных с ними сущностей,
        где поле modified > modified, которое хранится в redis (modified последней записи из запроса)
        Как только запрос не вернул результатов в redis попадает datetime.min, что позволяет циклу в etl пройтись по
        фильмам заново
        """
        self.check_connection_exists()
        last_modified = self.redis_storage.get_key('movie_last_modified')
        if last_modified is None:
            self.redis_storage.set_key('movie_last_modified', datetime.min)
            last_modified = datetime.min

        with self.connection.cursor() as cursor:
            cursor.execute(f"""
                    SELECT
                        fw.id,
                        fw.title,
                        fw.description,
                        fw.rating,
                        fw.type,
                        fw.created,
                        fw.modified,
                        COALESCE (
                            json_agg(
                                DISTINCT jsonb_build_object(
                                    'id', p.id,
                                    'name', p.full_name
                                )
                            ) FILTER (WHERE p.id is not null and pfw.role = 'actor'),
                            '[]'
                        ) as actors,
                        COALESCE (
                            json_agg(
                                DISTINCT jsonb_build_object(
                                    'id', p.id,
                                    'name', p.full_name
                                )
                            ) FILTER (WHERE p.id is not null and pfw.role = 'writer'),
                            '[]'
                        ) as writers,
                        array_agg(DISTINCT g.name) as genre,
                        GREATEST(fw.modified, MAX(p.modified), MAX(g.modified)) as greatest_modified,
                        coalesce(array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'director' ),
                         '{'{}'}') as director,
                        coalesce(array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'writer' ),
                         '{'{}'}') as writers_names,
                        coalesce(array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'actor' ),
                         '{'{}'}') as actors_names
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person p ON p.id = pfw.person_id
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre g ON g.id = gfw.genre_id
                    WHERE fw.modified > '{last_modified}'
                    GROUP BY fw.id
                    ORDER BY fw.modified
                    LIMIT {BATCH_SIZE};
            """)
            data = cursor.fetchall()
            try:
                last_modified = data[-1][6]
            except IndexError:
                last_modified = datetime.min
            self.redis_storage.set_key('movie_last_modified', last_modified)

        return data
