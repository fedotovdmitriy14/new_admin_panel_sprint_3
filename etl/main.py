import logging
from time import sleep

from elastic_loader import ElasticLoader
from postgres_extractor import PostgresExtractor
from settings import postgres_dsl, redis_config, elastic_config, LOGGER_SETTINGS, INDEXES
from state import RedisState


def etl_process() -> None:
    """
    Создаются экземпляры RedisState, PostgresExtractor, ElasticLoader
    В бесконечном цикле обходятся все записи из постгрес, и отправляются в elastic_loader
    """
    redis_storage = RedisState(redis_config)
    postgres_extractor = PostgresExtractor(postgres_dsl, redis_storage)
    elastic_loader = ElasticLoader(elastic_config)
    while True:
        for index_name, index_params in INDEXES.items():
            model = index_params[1]
            query = index_params[0]
            data = postgres_extractor.extract_data_from_db(index_name=index_name, query_function=query)
            if data:
                elastic_loader.update_elasticsearch(data, index_model=model, index_name=index_name)
                continue
            sleep(15.0)


if __name__ == '__main__':
    logging.basicConfig(**LOGGER_SETTINGS)
    logger = logging.getLogger(__name__)
    logger.info("ETL started")
    etl_process()
