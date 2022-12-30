from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

from state import RedisState


class PostgresExtractor:
    def __init__(self, dsl: dict, redis_storage: RedisState):
        self.dsl = dsl
        self.connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        self.redis_storage = redis_storage
        # self.redis_storage.set_key('last_modified', datetime.min)

    def extract_data_from_db(self):
        last_modified = self.redis_storage.get_key('last_modified')
        if last_modified is None:
            self.redis_storage.set_key('last_modified', datetime.min)

        cursor = self.connection.cursor()
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
                                'person_role', pfw.role,
                                'person_id', p.id,
                                'person_name', p.full_name
                            )
                        ) FILTER (WHERE p.id is not null and pfw.role = 'actor'),
                        '[]'
                    ) as actors,
                    COALESCE (
                        json_agg(
                            DISTINCT jsonb_build_object(
                                'person_role', pfw.role,
                                'person_id', p.id,
                                'person_name', p.full_name
                            )
                        ) FILTER (WHERE p.id is not null and pfw.role = 'writer'),
                        '[]'
                    ) as writers,
                    COALESCE (
                        json_agg(
                            DISTINCT jsonb_build_object(
                                'person_role', pfw.role,
                                'person_id', p.id,
                                'person_name', p.full_name
                            )
                        ) FILTER (WHERE p.id is not null and pfw.role = 'director'),
                        '[]'
                    ) as director,
                    array_agg(DISTINCT g.name) as genres,
                    array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'actor' ) as actors_names,
                    array_agg(DISTINCT p.full_name) FILTER ( WHERE pfw.role = 'writer' ) as writers_names
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
