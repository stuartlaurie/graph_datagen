import csv
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def create_rel_header(data_dir, rel_label):
    filename=data_dir+"/"+rel_label+"RelHeaders.csv"

    with open(filename, 'w', newline='') as rel_headers:
        csv_writer = csv.writer(rel_headers)
        csv_writer.writerow([
            ':START_ID(User)',
            ':END_ID(User)',
            'date:date'
        ])

    return filename

def create_rel_csv(filename, start_id, no_rels, total_rels, label, no_nodes, start_label, output_format):

    print(filename, start_id, no_rels, total_rels, label, no_nodes, start_label)

    rel_csv=open(filename, 'w', newline='')
    csv_writer = csv.writer(rel_csv)

    for i in range(start_id, start_id + no_rels +1):

        start_node=start_label+str(random.randint(1,no_nodes))
        end_node=start_label+str(random.randint(1,no_nodes))

        day=str(random.randint(1,29)).zfill(2)
        date="2020-06-"+day

        csv_writer.writerow([start_node,end_node,date])

    rel_csv.close()

    return filename


def create_rel_data(filename, start_id, no_rels, total_rels, label, no_nodes, start_label, output_format):

    data = []
    for i in range(start_id, start_id + no_rels +1):

        start_node=start_label+str(random.randint(1,no_nodes))
        end_node=start_label+str(random.randint(1,no_nodes))

        day=str(random.randint(1,29)).zfill(2)
        date="2020-06-"+day

        data.append([start_node,end_node,date])

    if (output_format == "parquet"):
        df = pd.DataFrame(data, columns=['START_ID','END_ID','DATE'])
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
    else:
        print("gotta write csv")

    return filename
