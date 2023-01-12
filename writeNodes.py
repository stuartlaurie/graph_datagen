import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from helpers import *

def create_node_header(data_dir, config):
    filename=data_dir+"/"+config['label']+"Node_Headers.csv"

    header=['id:ID('+config['label']+')']
    filename=writeImportHeader(filename,header,config)

    return filename

def create_node_data(filename, output_format, start_id, no_nodes, label, config):

    ## randomly generate node data
    data = []
    for i in range(start_id, start_id+no_nodes):
        list = []
        list.append(label+str(i))

        if "labels" in config:
            list=generateLabels(list,config['labels'])

        if "properties" in config:
            list=generateProperties(list,config['properties'])

        data.append(list)

    column_header=setColumnHeader(['START_ID'],config)
    df = pd.DataFrame(data, columns=column_header)

    if (output_format == "parquet"):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
    elif (output_format == "gzip"):
        df.to_csv(filename, index=False, header=False, compression="gzip")
    else:
        df.to_csv(filename, index=False, header=False)

    return filename
