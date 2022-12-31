from time import sleep

from elastic_loader import ElasticLoader
from postgres_extractor import PostgresExtractor
from settings import dsl, redis_config, elastic_config
from state import RedisState


def etl_process():
    redis_storage = RedisState(redis_config)
    postgres_extractor = PostgresExtractor(dsl, redis_storage)
    elastic_loader = ElasticLoader(elastic_config, RedisState(redis_config))
    while True:
        data = postgres_extractor.extract_data_from_db()
        if data:
            elastic_loader.update_elasticsearch(data)
            continue
        sleep(10.0)


if __name__ == '__main__':
    etl_process()
