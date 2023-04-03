import copy
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import time
import psycopg2 as pg

ENGINE_CMD_FMT = """postgresql://{USER}:{PWD}@{HOST}:{PORT}/{DB}"""

USER="ceb"
DBHOST="localhost"
PORT=5432
PWD="password"
DROP_TEMPLATE = "DROP TABLE IF EXISTS {TABLE_NAME}"
NEW_NAME_FMT = "{INP}_{DATA_KIND}"

def upload_to_postgres(df, dbname, table, data_kind,
        shuffle=True, null_strs=False):
    start = time.time()

    # shuffle the table to upload
    if shuffle:
        df = df.sample(frac=1.0)

    if null_strs:
        for k in df.keys():
            df[k] = df[k].apply(lambda x: x if pd.notnull(x) else
                    ''.join(random.choice(string.ascii_uppercase
                        + string.digits) for _ in range(random.randint(1,50))))

        print("done updating NULL values w/ random strings")

    new_table_name = NEW_NAME_FMT.format(INP=table,
            DATA_KIND=data_kind)
    ## drop table if needed
    drop_sql = DROP_TEMPLATE.format(TABLE_NAME = new_table_name)
    con = pg.connect(user=USER, host=DBHOST, port=PORT,
            password=PWD, database=dbname)
    cursor = con.cursor()
    cursor.execute(drop_sql)

    con.commit()
    cursor.close()
    con.close()

    engine_cmd = ENGINE_CMD_FMT.format(USER = USER,
                                       PWD = PWD,
                                       HOST = DBHOST,
                                       PORT = PORT,
                                       DB = dbname)

    engine = create_engine(engine_cmd)
    df.to_sql(new_table_name, engine)

    print("uploading to postgres took: ", round(time.time()-start, 2))

def apply_leftover_constraints(full_solution, leftover_constraints):
    for lc in leftover_constraints:
        if len(lc) == 1:
            new_vs = full_solution[lc[0][0]]
            for i, v in enumerate(full_solution[lc[0][0]]):
                if v is None:
                    new_vs = new_vs[:i] + [lc[0][2]] + new_vs[i + 1:]
                    break
            full_solution[lc[0][0]] = new_vs


def solution_to_df(full_solution):
    solution_df = pd.DataFrame(full_solution)
    solution_df = solution_df.applymap(lambda x: np.nan if x is None else x)
    return solution_df


def scale_solution_df(solution_df, table_cardinality, vars_per_col):
    real_row_size = int(table_cardinality / vars_per_col)
    newdf = pd.DataFrame(np.repeat(solution_df.values, real_row_size, axis=0))
    newdf.columns = solution_df.columns
    final_df = newdf.reindex(list(range(0, table_cardinality))).reset_index(drop=True)
    return final_df
