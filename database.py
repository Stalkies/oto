from typing import List

import asyncpg
from asyncpg import UniqueViolationError
from asyncpg.types import Point
import json

from config import Config


class DataBase:

    def __init__(self):
        self.pool = None

    async def create_pool(self) -> None:
        try:
            self.pool = await asyncpg.create_pool(Config.POSTGRES_URI)
            async with self.pool.acquire() as connection:
                await connection.execute('SELECT 1')
            print("\tSuccessful connection to the database")
        except Exception as e:
            print(f"\tDatabase connection error: {e}")

    async def add_car(self, car: dict) -> None:
        async with self.pool.acquire() as connection:
            columns = ", ".join(car.keys())
            values = tuple(car.values())
            query = f"INSERT INTO car ({columns}) VALUES {values}"
            try:
                await connection.execute(query)
            except UniqueViolationError:
                pass
