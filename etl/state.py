import abc
from typing import Any, Optional

import redis


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
    def get_state(self, key: str) -> Any:
         """Получить состояние по определённому ключу"""
         pass


class RedisState(State):
    def __init__(self, redis_config: dict, redis_connection: Optional[redis.Redis] = None):
        self.redis_config = redis_config
        self.redis_connection = redis_connection

    def check_connection(self) -> bool:
        try:
            self.redis_connection.ping()
        except:
            return False
        return True

    def get_connection(self) -> None:
        if self.redis_connection and self.check_connection():
            return self.redis_connection
        self.redis_connection = redis.Redis(**self.redis_config)

    def set_key(self, key: str, value: Any) -> None:
        """Сохраняет ключ и его значение в Redis"""
        self.get_connection()
        self.redis_connection.set(key, value.encode())

    def get_key(self, key: str) -> Any:
        """Получение значения по переданному ключу"""
        self.get_connection()
        value = self.redis_connection.get(key)
        if value:
            value = value.decode()
        return value
