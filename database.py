from typing import List

import asyncpg
from asyncpg import UniqueViolationError
from asyncpg.types import Point
import json
from config import Config
from utils import generate_data_types
import sys


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
            sys.exit()
            
    

    async def create_table(self, cars) -> None:
        async with self.pool.acquire() as connection:
            data_types = generate_data_types(cars)
            query = f"CREATE TABLE if not exists car(id BIGSERIAL NOT NULL PRIMARY KEY,"
            for column_name, data_type in data_types.items():
                query += f"{column_name} {data_type}, "
            query = query.rstrip(", ") + ")"
            try:
                await connection.execute(query)
            except Exception as _ex:
                print('\t[ERROR] Creating table failed:\n', _ex)
                

    async def add_car(self, car: dict) -> None:
        async with self.pool.acquire() as connection:
            columns = ", ".join(car.keys())
            values = tuple(car.values())
            query = f"INSERT INTO car ({columns}) VALUES {values}"
            try:
                await connection.execute(query)
            except UniqueViolationError:
                pass
            except Exception as ex:
                print(query)
                print(ex)
                sys.exit()
