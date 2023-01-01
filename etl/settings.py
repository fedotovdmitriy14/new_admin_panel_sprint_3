import os

from dotenv import load_dotenv
from pydantic import Field, BaseSettings

load_dotenv()


BACKOFF_MAX_TRIES = os.environ.get('BACKOFF_MAX_TRIES')


class PosgresDsl(BaseSettings):
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


POSTGRES_DSL = PosgresDsl()
REDIS_CONFIG = RedisConfig()
ELASTIC_CONFIG = ElasticConfig()
