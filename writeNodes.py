import csv
import random
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def create_node_header(data_dir):
    datafilepath=data_dir+"/Node"
    filename=datafilepath+"Headers.csv"
    ## Create Header file
    with open(filename, 'w', newline='') as node_headers:
        csv_writer = csv.writer(node_headers)
        csv_writer.writerow([
            'id:ID(User)',
        #    'ageGroup','gender','country'
        ])

    return filename

def create_node_csv(filename, start_id, no_nodes, label):

    node_csv = open(filename, 'w', newline='')
    csv_writer = csv.writer(node_csv)

    age_list=["15-21","12-30","30-39","40-49","50-59","60+"]
    gender_list=["M","F","NB"]
    country_list=["UK","US","APAC"]

    ## randomly generate node data
    for i in range(start_id, start_id+no_nodes+1):
        id=label+str(i)

        # age=age_list[random.randint(0,len(age_list)-1)]
        # gender=gender_list[random.randint(0,len(gender_list)-1)]
        # country=country_list[random.randint(0,len(country_list)-1)]
        # lastFavoriteGame=random.randint(1,300)
        # lastFavoriteGame90d=random.randint(1,300)

        csv_writer.writerow([
            id,
        #    age, gender, country
        ])

    node_csv.close()

    return filename


def create_node_data(filename, start_id, no_nodes, label, output_format):

    age_list=["15-21","12-30","30-39","40-49","50-59","60+"]
    gender_list=["M","F","NB"]
    country_list=["UK","US","APAC"]


    ## randomly generate node data
    data = []
    for i in range(start_id, start_id+no_nodes+1):
        data.append([label+str(i)])

    if (output_format == "parquet"):
        df = pd.DataFrame(data, columns=['Id'])
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
    else:
        print("gotta write csv")

    return filename
