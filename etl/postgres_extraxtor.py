import datetime

import psycopg2
from psycopg2.extras import DictCursor

from settings import dsl


class PostgresExtractor:
    def __init__(self, dsl: dict):
        self.dsl = dsl
        self.connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        self.film_modified = None

    def get_film_modified(self):
        with self.connection.cursor() as curs:
            curs.execute("""
                SELECT modified
                FROM content.film_work
                ORDER BY modified
                LIMIT 1
            """)
            return curs.fetchone()[0]

    def extract_data_from_db(self):
        if self.film_modified is None:
            self.film_modified = self.get_film_modified()
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
                    ) as actors_names,
                    COALESCE (
                        json_agg(
                            DISTINCT jsonb_build_object(
                                'person_role', pfw.role,
                                'person_id', p.id,
                                'person_name', p.full_name
                            )
                        ) FILTER (WHERE p.id is not null and pfw.role = 'writer'),
                        '[]'
                    ) as writers_names,
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
                    array_agg(DISTINCT g.name) as genres
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                WHERE fw.modified >= '{self.film_modified}'
                GROUP BY fw.id
                ORDER BY fw.modified
                LIMIT 100;
        """)
        data = cursor.fetchall()
        self.film_modified = data[-1][6]
        return data


p = PostgresExtractor(dsl)
print(p.film_modified)
p.extract_data_from_db()
print(p.film_modified)
