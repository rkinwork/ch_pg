import requests
import psycopg
from pathlib import Path
from dataclasses import dataclass, asdict
import logging
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True)
class ChCreds:
    host: str
    user: str = 'default'
    password: [str] = None


@dataclass(frozen=True)
class PgCreds:
    host: str
    user: str
    password: str
    dbname: str
    port: int = 5433


COPY_STMT = "COPY {0} FROM STDIN WITH CSV"


class CHPGTransfer:
    def __init__(
            self,
            ch_creds: ChCreds,
            pg_creds: PgCreds,
    ):
        self._ch_creds = ch_creds
        self._pg_creds = pg_creds

    def transfer(
            self,
            ch_query: str,
            prepare_tables: str,
            transfer_query: str,
            target_table: str,
            ch_query_params: [dict] = None,
    ):
        ch_query_params = ch_query_params or {}
        with psycopg.connect(
                autocommit=True,
                **asdict(self._pg_creds),
        ) as conn:
            with conn.cursor() as cur:
                logger.info(f"prepare tables")
                cur.execute(prepare_tables)
                logger.info(f"transfer data to stg")
                r = requests.post(
                    self._ch_creds.host,
                    data=ch_query,
                    params={
                        **ch_query_params,
                    },
                    headers={
                        'X-ClickHouse-User': self._ch_creds.user,
                        'X-ClickHouse-Key': self._ch_creds.password,
                        'X-ClickHouse-Format': 'CSV',
                    },
                    stream=True,
                )
                r.raise_for_status()

                with cur.copy(COPY_STMT.format(target_table)) as copy:
                    for chunk in r.iter_content(chunk_size=1024):
                        copy.write(chunk)

                r.close()
                logger.info(f"transfer data to reports")
                cur.execute(transfer_query)


def main():
    tr = CHPGTransfer(
        ch_creds=ChCreds(
            host=os.getenv('CH_HOST', None),
            user=os.getenv('CH_USER', None),
            password=os.getenv('CH_PASS', None),
        ),
        pg_creds=PgCreds(
            host=os.getenv('PG_HOST', None),
            user=os.getenv('PG_USER', None),
            password=os.getenv('PG_PASS', None),
            dbname=os.getenv('PG_DBNAME', None),
        )

    )
    rt = Path(__file__).parent
    tr.transfer(
        ch_query=rt.joinpath('./sql/extract_ch.sql').read_text(),
        prepare_tables=rt.joinpath('./sql/prepare_tables.sql').read_text(),
        transfer_query=rt.joinpath('./sql/transfer_query.sql').read_text(),
        target_table='finance.rides_stg',
        ch_query_params={
            'param_dt': '2015-08-01'
        }
    )


if __name__ == '__main__':
    main()
