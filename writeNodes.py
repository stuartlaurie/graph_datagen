import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
from helpers import *
from faker import Faker
fake = Faker()

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
        ## check efficiency - generate list first then pull values?
        if "properties" in config:
            for properties in config['properties']:
                if properties['type'] == 'date':
                    start_date=datetime.date(year=properties['lower']['year'], month=properties['lower']['month'], day=properties['lower']['day'])
                    end_date=datetime.date(year=properties['upper']['year'], month=properties['upper']['month'], day=properties['upper']['day'])
                    list.append(fake.date_between(start_date=start_date, end_date=end_date))
                if properties['type'] == 'int':
                    list.append(random.randint(properties['lower'],properties['upper']))

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
