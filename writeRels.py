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

def create_rel_data(filename, output_format, start_id, no_rels, label, config, nodeidrange, generalconfig):

    data = []
    df_chunk=generalconfig['df_row_limit']
    df_chunk_no=1
    column_header=setColumnHeader(['START_ID','END_ID'],config)

    for i in range(start_id, start_id+no_rels):

        list = []
        list.append(config['source_node_label']+str(random.randint(nodeidrange[config['source_node_label']]['lower'],nodeidrange[config['source_node_label']]['upper'])))
        list.append(config['target_node_label']+str(random.randint(nodeidrange[config['source_node_label']]['lower'],nodeidrange[config['source_node_label']]['upper'])))

        if "properties" in config:
            list=generateProperties(list,config['properties'])

        data.append(list)

        if i % df_chunk == 0:
            write_to_file(filename,output_format,data,column_header,df_chunk_no)
            df_chunk_no+=1
            data=[]

    write_to_file(filename,output_format,data,column_header,df_chunk_no)

    return filename
