import csv
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def create_rel_header(data_dir, rel_config):
    filename=data_dir+"/"+rel_config['label']+"RelHeaders.csv"

    with open(filename, 'w', newline='') as rel_headers:
        csv_writer = csv.writer(rel_headers)
        csv_writer.writerow([
            ':START_ID('+rel_config['source_node_label']+')',
            ':END_ID('+rel_config['target_node_label']+')',
            'date:date'
        ])

    return filename

def create_rel_data(filename, output_format, start_id, no_rels, label, config, nodelabelcount):

    data = []
    for i in range(start_id, start_id+no_rels):

        start_node=config['source_node_label']+str(random.randint(1,nodelabelcount[config['source_node_label']]))
        end_node=config['target_node_label']+str(random.randint(1,nodelabelcount[config['target_node_label']]))

        day=str(random.randint(1,29)).zfill(2)
        date="2020-06-"+day

        data.append([start_node,end_node,date])

    df = pd.DataFrame(data, columns=['START_ID','END_ID','DATE'])

    if (output_format == "parquet"):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
    elif (output_format == "gzip"):
        df.to_csv(filename, index=False, header=False, compression="gzip")
    else:
        df.to_csv(filename, index=False, header=False)


    return filename
