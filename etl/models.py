from typing import Optional, List
from uuid import UUID

from pydantic import BaseModel, Field


class AbstractModel(BaseModel):
    id: UUID


class PersonShort(AbstractModel):
    name: str


class Person(PersonShort):
    roles: Optional[List[str]] = None
    film_ids: Optional[List[str]] = None


class FilmWork(AbstractModel):
    title: str
    description: Optional[str] = None
    imdb_rating: float = Field(alias='rating')
    actors: Optional[List[PersonShort]] = None
    writers: Optional[List[PersonShort]] = None
    director: Optional[List] = None
    genre: Optional[List] = None
    actors_names: Optional[List] = None
    writers_names: Optional[List] = None


class Genre(AbstractModel):
    name: str
