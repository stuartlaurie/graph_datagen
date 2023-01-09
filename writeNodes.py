import csv
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def create_node_header(data_dir,node_label):
    filename=data_dir+"/"+node_label+"Node_Headers.csv"
    ## Create Header file
    with open(filename, 'w', newline='') as node_headers:
        csv_writer = csv.writer(node_headers)
        csv_writer.writerow([
            'id:ID('+node_label+')',
        #    'ageGroup','gender','country'
        ])

    return filename

def create_node_data(filename, output_format, start_id, no_nodes, label):

    ## randomly generate node data
    data = []
    for i in range(start_id, start_id+no_nodes):
        data.append([label+str(i)])

    df = pd.DataFrame(data, columns=['Id'])

    if (output_format == "parquet"):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
    elif (output_format == "gzip"):
        df.to_csv(filename, index=False, header=False, compression="gzip")
    else:
        df.to_csv(filename, index=False, header=False)

    return filename
