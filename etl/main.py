import logging
from time import sleep

from elastic_loader import ElasticLoader
from postgres_extractor import PostgresExtractor
from settings import POSTGRES_DSL, REDIS_CONFIG, ELASTIC_CONFIG, LOGGER_SETTINGS
from state import RedisState


def etl_process():
    redis_storage = RedisState(REDIS_CONFIG)
    postgres_extractor = PostgresExtractor(POSTGRES_DSL, redis_storage)
    elastic_loader = ElasticLoader(ELASTIC_CONFIG, redis_storage)
    while True:
        data = postgres_extractor.extract_data_from_db()
        if data:
            elastic_loader.update_elasticsearch(data)
            continue
        sleep(10.0)


if __name__ == '__main__':
    logging.basicConfig(**LOGGER_SETTINGS)
    logger = logging.getLogger(__name__)
    logger.info("ETL started")
    etl_process()
