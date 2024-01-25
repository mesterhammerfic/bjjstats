import abc
import dataclasses
from typing import List, Sequence, Optional

from psycopg import Connection, connect
from psycopg.rows import dict_row

class DataModel(abc.ABC):
    @abc.abstractmethod
    def close(self):
        """
        closes the connection to the database
        """
        ...

    @abc.abstractmethod
    def add_athlete(self, name: str, nickname: Optional[str]) -> int:
        """
        adds a new athlete entry to the database
        """
        ...


class PostgresDataModel(DataModel):
    """
    The postgres implementation of the DataModel
    """
    def __init__(self, db_url: str):
        self._connection = connect(
            db_url,
            row_factory=dict_row,
        )

    def close(self):
        self._connection.close()

    def add_athlete(self, name: str, nickname: Optional[str]) -> int:
        new_id = self._connection.execute(
            (
                "INSERT INTO athlete (name, nickname)"
                " VALUES (%s, %s)"
                " RETURNING id"
            ),
            (name, nickname),
        ).fetchone()
        self._connection.commit()
        return new_id["id"]
