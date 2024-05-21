import duckdb
from pathlib import Path

qry = """with dm as (
select CAST(generate_series AS DATE) as dt 
FROM generate_series(DATE '2022-01-01', DATE '2022-05-01', interval '1 month')
                                     )
select dt, game_id, term_type, amount, bucket_from, bucket_to 
from dm left join terms_dict td 
on td.valid_from <= dm.dt and td.valid_to >=  dm.dt"""

terms_file = Path(__file__).parent.joinpath('terms.csv')
dest_file = Path(__file__).parent.joinpath('dest.csv')


def main():
    con = duckdb.connect()
    terms_dict = con.read_csv(terms_file.as_posix(), delimiter=';')
    ref = con.sql(qry)
    ref.to_csv(dest_file.as_posix())


if __name__ == '__main__':
    main()
