import logging
from typing import Tuple, List

import asyncpg
from asyncpg.pool import Pool
from django.db.models import QuerySet


logger = logging.getLogger('AsyncPgClient')


class AsyncPgClient:

    def __init__(self, host: str, port: int, user: str, password: str, database: str, use_django_conn=False) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

        self.use_django_conn = use_django_conn  # for django unit tests

        self.connection_pool: Pool = None

    async def async_init(self) -> None:

        self.connection_pool = await asyncpg.create_pool(host=self.host, port=self.port,
                                                         user=self.user, password=self.password, database=self.database,
                                                         min_size=10, max_size=30)

    async def exec(self, django_queryset: QuerySet, values: tuple) -> List[dict]:

        django_queryset = django_queryset.values(*values)
        sql, params = self._get_sql(django_queryset)

        if self.use_django_conn:
            logger.warning('AsyncPgDbClient uses sync django connection. Do not use it in a production')
            return list(django_queryset)

        async with self.connection_pool.acquire() as connection:
            records = await connection.fetch(sql, *params)

        result = []
        for record in records:
            result.append({val: record[i] for i, val in enumerate(values)})
        return result

    @staticmethod
    def _get_sql(qs: QuerySet) -> Tuple[str, tuple]:
        compiler = qs.query.get_compiler(using=qs.db)
        sql, sql_params = compiler.as_sql()

        for i in range(len(sql_params)):  # %s, %s  ->  $1, $2
            sql = sql.replace('%s', f'${i+1}', 1)

        return sql, sql_params

