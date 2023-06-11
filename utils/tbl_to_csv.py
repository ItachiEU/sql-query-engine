import pandas as pd
import json
import argparse

def convert_tbl_to_csv(tbl_file_path, csv_file_path, column_names, relation_name):
    df = pd.read_csv(tbl_file_path, sep='|', names=column_names, index_col=False)

    df.to_csv(csv_file_path, index=False)

if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Tbl to json converter')
   parser.add_argument('--path', type=str, help='Provide path to tbl file', required=True)

   args = parser.parse_args()
   relation_name = args.path.split('/')[-1].split('.')[0].upper()
   with open('tpc-h.json', 'r') as dd_file:
      dd = json.load(dd_file)
      column_names = [f"{relation_name}.{key}" for key in dd[relation_name].keys()]
   
   convert_tbl_to_csv(args.path, f'data/{relation_name}.csv', column_names, relation_name)