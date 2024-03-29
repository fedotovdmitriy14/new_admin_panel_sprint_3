def get_movie_query(batch_size: str, last_modified: str) -> tuple:
    """Формирование sql запроса для movies"""

    movie_query = f"""
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
            COALESCE (
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', g.id,
                        'name', g.name
                    )
                ),
                '[]'
            ) as genre,
            array_agg(DISTINCT g.name) as genre_names,
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
        LIMIT {batch_size};
    """

    last_modified_index = 6
    return movie_query, last_modified_index


def get_genre_query(batch_size: str, last_modified: str) -> tuple:
    """Формирование sql запроса для genres"""

    genre_query = f"""
        SELECT
            g.id,
            g.name,
            g.modified
        FROM content.genre as g
        WHERE g.modified > '{last_modified}'
        GROUP BY g.id
        ORDER BY g.modified
        LIMIT {batch_size};
    """

    last_modified_index = 2
    return genre_query, last_modified_index


def get_person_query(batch_size: str, last_modified: str) -> tuple:
    person_query = f"""
        SELECT
            p.id,
            p.full_name as name,
            array_agg(DISTINCT pfw.role) as roles,
            array_agg(DISTINCT pfw.film_work_id::text) as film_ids,
            p.modified
        FROM content.person p
        LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
        WHERE p.modified >'{last_modified}'
        GROUP BY p.id
        ORDER BY p.modified
        LIMIT {batch_size};
      """
    last_modified_index = 4
    return person_query, last_modified_index
