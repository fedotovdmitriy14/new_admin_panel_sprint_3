import abc
import pickle
from typing import Any, Optional

import backoff
import redis

from settings import BACKOFF_MAX_TRIES, redis_config


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """
    @abc.abstractmethod
    def set_key(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        pass

    @abc.abstractmethod
    def get_key(self, key: str) -> Any:
         """Получить состояние по определённому ключу"""
         pass


class RedisState(State):
    def __init__(self, redis_config: dict, redis_connection: Optional[redis.Redis] = None):
        self.redis_config = redis_config
        self.redis_connection = redis_connection

    def is_connection_alive(self) -> bool:
        try:
            self.redis_connection.ping()
        except:
            return False
        return True

    def check_connection_exists(self) -> None:
        """Проверяется наличие соединения к Redis"""
        if self.redis_connection and self.is_connection_alive():
            return self.redis_connection
        self.redis_connection = self.create_connection()

    @backoff.on_exception(backoff.expo, Exception, max_tries=BACKOFF_MAX_TRIES)
    def create_connection(self) -> redis.Redis:
        """Создается новое соединение к Redis"""
        return redis.Redis(**self.redis_config)

    @backoff.on_exception(backoff.expo, Exception, max_tries=BACKOFF_MAX_TRIES)
    def set_key(self, key: str, value: Any) -> None:
        """Сохраняется ключ и его значение в Redis"""
        self.check_connection_exists()
        self.redis_connection.set(key, pickle.dumps(value))

    @backoff.on_exception(backoff.expo, Exception, max_tries=BACKOFF_MAX_TRIES)
    def get_key(self, key: str) -> Any:
        """Получение значения по переданному ключу"""
        self.check_connection_exists()
        value = self.redis_connection.get(key)
        if value:
            value = pickle.loads(value)
        return value
