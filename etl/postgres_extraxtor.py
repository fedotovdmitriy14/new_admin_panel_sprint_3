from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

from settings import dsl


class PostgresExtractor:
    def __init__(self, dsl: dict):
        self.dsl = dsl
        self.connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        self.film_modified_min = None

    def extract_data_from_db(self):
        if self.film_modified_min is None:
            self.film_modified_min = datetime.min
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
                WHERE fw.modified > '{self.film_modified_min}'
                GROUP BY fw.id
                ORDER BY fw.modified
                LIMIT 100;
        """)
        data = cursor.fetchall()
        self.film_modified_min = data[-1][6]
        return data


p = PostgresExtractor(dsl)
print(p.film_modified_min)
p.extract_data_from_db()
print(p.film_modified_min)
p.extract_data_from_db()
print(p.film_modified_min)

