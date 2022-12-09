from sqlglot import parse, exp
import argparse
import json
import pandas as pd
parser = argparse.ArgumentParser(
    description="Analyze the workload distribution.")
parser.add_argument("-f", "--input_file", type=str,
                    help="Input SQL file", default="workload.sql")
parser.add_argument("-d", "--dialect", type=str,
                    help="SQL file dialect", default="mysql")
parser.add_argument("-o", "--output_file", type=str,
                    help="Output QCS stat file", default="qcs.csv")
args = parser.parse_args()

input_file = args.input_file
dialect = args.dialect
output_file = args.output_file

fi = open(input_file, 'r')

qcs_history = pd.DataFrame(columns=['query_time', 'query_col', 'template'])

count = 1
while True:
    raw = fi.readline()
    if not raw:
        break
    template_id, sql = raw.split('|')
    parsed_stmts = parse(sql, read=dialect)
    qcs = set()
    for stmt in parsed_stmts:
        for expr_class in [exp.Where, exp.Having, exp.Group]:
            for expr in stmt.find_all(expr_class):
                for column in expr.find_all(exp.Column):
                    qcs.add(column.alias_or_name)
    for col in qcs:
        qcs_history = qcs_history.append(
            {"query_time": count, "query_col": col, "template": template_id}, ignore_index=True)
    count += 1

qcs_history.to_csv(output_file, index=False)
