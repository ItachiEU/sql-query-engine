from typing import Callable, Union
import radb
import radb.ast
import radb.parse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.rdd
import sqlparse
import sql2ra
import raopt

def extract_conditions(cond):
   if isinstance(cond, radb.ast.ValExprBinaryOp) and cond.op == radb.parse.RAParser.AND:
          left_conditions = extract_conditions(cond.inputs[0])
          right_conditions = extract_conditions(cond.inputs[1])
          return left_conditions + right_conditions
   elif isinstance(cond, radb.ast.ValExprBinaryOp) and cond.op == radb.parse.RAParser.EQ:
       left = cond.inputs[0]
       right = cond.inputs[1]
       if isinstance(left, radb.ast.AttrRef) and isinstance(right, radb.ast.AttrRef):
           return [(f"{left.rel}.{left.name}", f"{right.rel}.{right.name}")]
   return []

def evaluate_condition(condition):
    if isinstance(condition, radb.ast.AttrRef):
        return lambda row: row[f"{condition.rel}.{condition.name}"]
    elif isinstance(condition, radb.ast.ValExprBinaryOp):
        left = evaluate_condition(condition.inputs[0])
        right = evaluate_condition(condition.inputs[1])

        if condition.op == radb.parse.RAParser.EQ:
            return lambda row: left(row) == right(row)
        elif condition.op == radb.parse.RAParser.AND:
            return lambda row: left(row) and right(row)
    elif isinstance(condition, radb.ast.ValExpr):
        if isinstance(condition, radb.ast.RAString):
            value = condition.val.strip("'")
            return lambda row: value
        else:
            value = condition.val
            return lambda row: value
    else:
        raise Exception("Unsupported condition type: {}".format(type(condition)))


def task_factory(raquery: radb.ast.Node, spark: SparkSession) -> Callable[[], pyspark.rdd.RDD]:
    if isinstance(raquery, radb.ast.Select):
        cond_func = evaluate_condition(raquery.cond)
        input_transformation = task_factory(raquery.inputs[0], spark)
        return lambda: input_transformation().filter(cond_func)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = f"./data/{raquery.rel}.csv"
        rdd = spark.sparkContext.textFile(filename)
        header_line = rdd.first()
        column_names = header_line.split(',')
        
        def parse_line(line):
            fields = line.split(',')
            return dict(zip(column_names, fields))
         
        rdd = rdd.filter(lambda line: line != header_line)
        return lambda : rdd.map(parse_line)
     
    elif isinstance(raquery, radb.ast.Join):
        left_transformation = task_factory(raquery.inputs[0], spark)
        right_transformation = task_factory(raquery.inputs[1], spark)
        
        conditions = extract_conditions(raquery.cond)

        left_rdd = left_transformation().map(lambda row: (tuple(row[col] for col, _ in conditions), row))
        right_rdd = right_transformation().map(lambda row: (tuple(row[col] for _, col in conditions), row))

        return lambda: left_rdd.join(right_rdd).map(lambda x: {**x[1][0], **x[1][1]})

    elif isinstance(raquery, radb.ast.Project):
        input_transformation = task_factory(raquery.inputs[0], spark)
        attrs = [f"{attr.rel}.{attr.name}" for attr in raquery.attrs]
        
        def to_tuple(row):
            return tuple(sorted(row.items()))

        def to_dict(tpl):
            return dict(tpl)
        
        return lambda: (input_transformation()
                    .map(lambda row: {col: row[col] for col in attrs})
                    .map(to_tuple)
                    .distinct()
                    .map(to_dict))

    elif isinstance(raquery, radb.ast.Rename):
        input_transformation = task_factory(raquery.inputs[0], spark)
        old_name = raquery.inputs[0].rel
        new_name = raquery.relname
        
        return lambda: input_transformation().map(
            lambda row: {f"{new_name}.{key.split('.')[1]}" if key.split('.')[0] == old_name else key: value for key, value in row.items()})

    else:
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")

     
def run_radb_query_in_spark(query: Union[str, radb.ast.Node]):
    spark = SparkSession.builder.getOrCreate()
    
    if isinstance(query, str):
        radb_query = radb.parse.one_statement_from_string(query)
    else:
        radb_query = query
        
    transformation = task_factory(radb_query, spark)
    result_rdd = transformation().collect()
    print(result_rdd, len(result_rdd))

   
def run_sql_query_in_spark(sqlstring: str, dd: dict = {}):
  if len(dd) == 0:
     dd["Person"] = {"name": "string", "age": "integer", "gender": "string"}
     dd["Eats"] = {"name": "string", "pizza": "string"}
     dd["Serves"] = {"pizzeria": "string", "pizza": "string", "price": "integer"}
  
  stmt = sqlparse.parse(sqlstring)[0]
  ra0 = sql2ra.translate(stmt)
  
  ra1 = raopt.rule_break_up_selections(ra0)
  ra2 = raopt.rule_push_down_selections(ra1, dd)

  ra3 = raopt.rule_merge_selections(ra2)
  ra4 = raopt.rule_introduce_joins(ra3)
  
  run_radb_query_in_spark(ra4)
