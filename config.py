from dataclasses import dataclass
from os import environ


@dataclass
class Config:
    message = 'define me!'

    pg_user: str = environ.get('PG_USER', message)
    pg_password: str = environ.get('PG_PASSWORD', message)
    pg_ip: str = environ.get('PG_IP', message)
    pg_database: str = environ.get('PG_DATABASE', message)
    POSTGRES_URI = f'postgresql://{pg_user}:{pg_password}@{pg_ip}/{pg_database}'
