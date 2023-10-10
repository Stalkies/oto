import asyncpg
from asyncpg import UniqueViolationError

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
            placeholders = ", ".join([f"${i + 1}" for i in range(len(car))])
            values = tuple(car.values())
            query = f"INSERT INTO car ({columns}) VALUES ({placeholders})"
            try:
                await connection.execute(query, *values)
            except UniqueViolationError as ex:
                print(query, values)
                print(ex)
            except Exception as ex:
                print(query, values)
                print(ex)

    async def add_cars(self, cars: list) -> None:
        async with self.pool.acquire() as connection:
            columns = ", ".join(cars[0].keys())
            values = [tuple(car.values()) for car in cars]
            placeholders = ", ".join(
                ["(" + ", ".join(["$" + str(i + 1) for i in range(len(car))]) + ")" for car in values])
            query = f"INSERT INTO car ({columns}) VALUES {placeholders}"
            try:
                await connection.executemany(query, values)
            except UniqueViolationError as ex:
                print(query, values)
                print(ex)
            except Exception as ex:
                print(query)
                print(ex)

    import asyncpg

    async def check_links_in_db(self, links: list) -> list:
        async with self.pool.acquire() as connection:
            query = "SELECT link FROM car WHERE link = ANY($1)"
            existing_links = await connection.fetch(query, links)
            existing_links_set = set(row["link"] for row in existing_links)
            new_links = [link for link in links if link not in existing_links_set]
            return new_links
