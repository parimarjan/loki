import argparse
import json
import logging
import sys

import copy
import numpy as np
import pandas as pd
import pdb

import loki

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run Loki.')
    parser.add_argument('--config', '-c', default='application.conf')
    parser.add_argument('--workload', '-w', default='imdb')
    parser.add_argument('--table', '-t', default='n')
    parser.add_argument('--vars_per_col', '-n', default=1000)
    parser.add_argument('--logfile', '-lf', default='logs/loki.log')
    parser.add_argument('--verbose', '-v', default=False, action='store_true')

    args = parser.parse_args()

    loki.load_config(args.config)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s [%(name)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(args.logfile), logging.StreamHandler()])

    logger = logging.getLogger('MAIN')

    logger.info('Loki initialized with configuration:\n%s' % json.dumps(loki.config, sort_keys=True, indent=4))

    from loki.util import constraints, postprocessing
    from loki.solver import sat

    df = pd.read_csv(loki.config[args.workload]['df'])
    tables = set(df['input'])
    table_columns = {table: set(df['column'].where(df['input'] == table).dropna()) for table in tables}
    c_df = pd.read_csv(loki.config[args.workload]['constraints_df'])
    c_df = c_df[~c_df["Op"].str.contains("like")]
    c_df = c_df[~c_df["Op"].str.contains("NOT")]
    c_df = c_df[~c_df["Op"].str.contains("!=")]
    c_df = c_df[~c_df["Op"].str.contains(">")]
    c_df = c_df[~c_df["Op"].str.contains("<")]
    print("Operators used: ", set(c_df["Op"]))
    c_df = c_df.sample(frac=0.1)
    print("Size of constraint df: ", len(c_df))

    # c_df = c_df[c_df["Selectivity"] != 1.0]
    # # pdb.set_trace()
    # maxcard = max(c_df["RowCount"].values)
    # c_df = c_df[c_df["RowCount"] < maxcard]
    # print(len(c_df))

    # print(table_columns)
    # pdb.set_trace()

    table = args.table

    orig_table = table
    # table = '"{}"'.format(table)

    columns = table_columns[table]

    constraints_df = constraints.get_constraints_df(c_df, table)

    table_cardinality = constraints.get_table_cardinality(constraints_df)
    co_optimized_columns = constraints.get_co_optimized_columns(constraints_df, columns)
    programs = constraints.get_programs(co_optimized_columns)

    leftover_constraints = []

    vars_per_col = args.vars_per_col

    solutions = []

    # print(constraints_df.head(5))
    print("removing Nones")
    constraints_df = constraints_df[constraints_df["Value0"] != "None"]
    constraints_df = constraints_df[constraints_df["Value1"] != "None"]
    constraints_df = constraints_df[constraints_df["Value"] != "None"]
    # pdb.set_trace()
    for program in programs:
        logger.info(f'Solving: {program}')
        constraints_ = constraints.parse_constraints(program, constraints_df)
        model, vars, cols, col_values_ids_map = sat.build_model(program, constraints_, leftover_constraints, table_cardinality, vars_per_col)
        solution = sat.solve(model, vars, cols, col_values_ids_map, vars_per_col)
        solutions.append(solution)
        logger.info(f'Solved: {program}')


    # Combine all programs' solutions into a single dictionary solution
    full_solution = {k: v for s in solutions for k, v in s.items()}

    postprocessing.apply_leftover_constraints(full_solution, leftover_constraints)
    solution_df = postprocessing.solution_to_df(full_solution)
    final_solution_df = postprocessing.scale_solution_df(solution_df, table_cardinality, vars_per_col)

    final_solution_df.to_csv(f'results/{orig_table}.csv')

    logger.info('Exiting Loki!')

    sys.exit(0)
