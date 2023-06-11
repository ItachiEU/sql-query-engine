import pandas as pd
import json
import argparse

def convert_tbl_to_json(tbl_file_path, json_file_path, column_names, relation_name):
    df = pd.read_csv(tbl_file_path, sep='|', names=column_names, index_col=False)

    df.columns = [f"{relation_name}.{col}" for col in df.columns]

    records = df.to_dict('records')
    
    with open(json_file_path, 'w') as f:
        for record in records:
            f.write(relation_name + '\t' + json.dumps(record) + '\n')

if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Tbl to json converter')
   parser.add_argument('--path', type=str, help='Provide path to tbl file', required=True)

   args = parser.parse_args()
   relation_name = args.path.split('/')[-1].split('.')[0].upper()
   with open('tpc-h.json', 'r') as dd_file:
      dd = json.load(dd_file)
      column_names = list(dd[relation_name].keys())
   
   convert_tbl_to_json(args.path, f'data/{relation_name}.json', column_names,relation_name)