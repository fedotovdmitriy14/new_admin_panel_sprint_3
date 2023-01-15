from typing import Optional, List

from pydantic import BaseModel, Field


class AbstractModel(BaseModel):
    id: Optional[str] = None


class Person(AbstractModel):
    name: Optional[str] = None


class FilmWork(AbstractModel):
    title: Optional[str] = None
    description: Optional[str] = None
    imdb_rating: Optional[float] = Field(alias='rating')
    actors: Optional[List[Person]] = None
    writers: Optional[List[Person]] = None
    director: Optional[List] = None
    genre: Optional[List] = None
    actors_names: Optional[List] = None
    writers_names: Optional[List] = None


class Genre(AbstractModel):
    name: Optional[str] = None
