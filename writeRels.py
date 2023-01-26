import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
from helpers import *

def create_rel_header(data_dir, config):
    filename=data_dir+"/"+config['label']+"RelHeaders.csv"

    header=[
        ':START_ID('+config['source_node_label']+')',
        ':END_ID('+config['target_node_label']+')',
    ]
    filename=writeImportHeader(filename,header,config)

    return filename

def create_rel_data(filename, output_format, start_id, no_rels, label, config, nodeidrange):

    data = []

    for i in range(start_id, start_id+no_rels):

        list = []
        list.append(config['source_node_label']+str(random.randint(nodeidrange[config['source_node_label']]['lower'],nodeidrange[config['source_node_label']]['upper'])))
        list.append(config['target_node_label']+str(random.randint(nodeidrange[config['source_node_label']]['lower'],nodeidrange[config['source_node_label']]['upper'])))

        if "properties" in config:
            list=generateProperties(list,config['properties'])

        data.append(list)

    column_header=setColumnHeader(['START_ID','END_ID'],config)
    df = pd.DataFrame(data, columns=column_header)

    if (output_format == "parquet"):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
    elif (output_format == "gzip"):
        df.to_csv(filename, index=False, header=False, compression="gzip")
    else:
        df.to_csv(filename, index=False, header=False)


    return filename
