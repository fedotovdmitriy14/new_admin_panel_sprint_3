import logging
import os

from dotenv import load_dotenv
from pydantic import Field, BaseSettings

from models import FilmWork, Genre, Person
from postgres_queries import get_movie_query, get_genre_query, get_person_query

load_dotenv()


BATCH_SIZE = os.environ.get('BATCH_SIZE', 500)

logger = logging.getLogger(__name__)

LOGGER_SETTINGS = {
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # noqa 501
    'datefmt': '%Y-%m-%d %H:%M:%S',
    'level': logging.INFO,
    'handlers': [logging.StreamHandler()],
}

INDEXES = {
    'movies': (get_movie_query, FilmWork),
    'genres': (get_genre_query, Genre),
    'persons': (get_person_query, Person)
}


class BackoffConfig(BaseSettings):
    backoff_max_tries: int = Field(env="BACKOFF_MAX_TRIES")


class PostgresDsl(BaseSettings):
    dbname: str = Field(env="DB_NAME")
    user: str = Field(env="DB_USER")
    password: str = Field(env="DB_PASSWORD")
    host: str = Field(env="DB_HOST")
    port: int = Field(env="DB_PORT")


class RedisConfig(BaseSettings):
    host: str = Field(env="REDIS_HOST")
    port: int = Field(env="REDIS_PORT")


class ElasticConfig(BaseSettings):
    host: str = Field(env="ELASTIC_HOST")
    port: int = Field(env="ELASTIC_PORT")


postgres_dsl = PostgresDsl()
redis_config = RedisConfig()
elastic_config = ElasticConfig()
backoff_config = BackoffConfig()
