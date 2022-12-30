from datetime import datetime
from typing import Optional, Dict, List
from uuid import UUID

from pydantic import BaseModel


class Person(BaseModel):
    # id: Optional[UUID] = None
    name: Optional[str] = None


class FilmWork(BaseModel):
    id: UUID
    title: Optional[str] = None
    description: Optional[str] = None
    imdb_rating: Optional[float] = None
    type: Optional[str] = None
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
    actors: Optional[List[Person]] = None
    writers: Optional[List[Person]] = None
    director: Optional[List[Person]] = None
    genre: Optional[str] = None
    actors_names: Optional[List] = None
    writers_names: Optional[List] = None
