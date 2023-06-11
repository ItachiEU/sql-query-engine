import argparse
import json
from ra2spark import ra2spark
from ra2mr import ra2mr


if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Sql engine')
   parser.add_argument('--mode', type=str, choices=['spark', 'hadoop'], default='spark', help='choose Spark vs Hadoop')
   parser.add_argument('--env', choices=['HDFS', 'LOCAL'], default='HDFS',
                     help='execution environment')
   parser.add_argument('--log_level', choices=['INFO', 'DEBUG', 'WARN', 'ERROR'], default='INFO', help='Logging level')
   parser.add_argument('--dd_path', type=str, help='Give path to a json file containing dictionary with relations information', default='')                        
   parser.add_argument('query', help='SQL query')

   args = parser.parse_args()
   dd = {}
   if args.dd_path:
      with open(args.dd_path, 'r') as dd_file:
         dd = json.load(dd_file)
         
   if args.mode == 'spark':
      ra2spark.run_sql_query_in_spark(args.query, args.env, dd, ll=args.log_level)
   else:
      ra2mr.run_sql_query_on_hadoop(ra2mr.ExecEnv.LOCAL if args.env == 'LOCAL' else ra2mr.ExecEnv.HDFS, args.query, dd)
