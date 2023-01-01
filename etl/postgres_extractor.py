from datetime import datetime

import backoff
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor

from settings import BACKOFF_MAX_TRIES, PosgresDsl
from state import RedisState


class PostgresExtractor:
    def __init__(self, dsl: PosgresDsl, redis_storage: RedisState):
        self.dsl = dsl
        self.connection = None
        self.redis_storage = redis_storage

    def check_connection_exists(self) -> None:
        """Создается новое соединения, если его нет или оно закрыто"""
        if self.connection is None or self.connection.closed:
            self.create_connection()

    @backoff.on_exception(backoff.expo, Exception, max_tries=BACKOFF_MAX_TRIES)
    def create_connection(self) -> connection:
        """Создается новое соединение"""
        self.connection = psycopg2.connect(**self.dsl.dict(), cursor_factory=DictCursor)
        return self.connection

    @backoff.on_exception(backoff.expo, Exception, max_tries=BACKOFF_MAX_TRIES)
    def extract_data_from_db(self):
        self.check_connection_exists()
        last_modified = self.redis_storage.get_key('last_modified')
        if last_modified is None:
            self.redis_storage.set_key('last_modified', datetime.min)

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
                        array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'actor' ) as actors_names,
                        array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'writer' ) as writers_names,
                        array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'director' ) as director,
                        GREATEST(fw.modified, MAX(p.modified), MAX(g.modified)) as greatest_modified
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person p ON p.id = pfw.person_id
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre g ON g.id = gfw.genre_id
                    WHERE fw.modified > '{last_modified}'
                    GROUP BY fw.id
                    ORDER BY fw.modified
                    LIMIT 500;
            """)
            data = cursor.fetchall()
            try:
                last_modified = data[-1][6]
            except IndexError:
                last_modified = datetime.min
            self.redis_storage.set_key('last_modified', last_modified)
            return data
