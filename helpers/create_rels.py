import random
from helpers.general_helpers import *
from helpers.admin_import import *
from helpers.create_data import *

def create_rel_header(data_dir, config):
    ## create the relationship header file
    filename=data_dir+"/"+config['label']+"RelHeaders.csv"

    header=[
        ':START_ID('+config['source_node_label']+')',
        ':END_ID('+config['target_node_label']+')',
    ]
    filename=write_import_header(filename,header,config)

    return filename


def create_rel_data(filename, output_format, start_id, no_rels, label, config, nodeidrange, generalconfig):

    data = []
    df_row_limit=generalconfig['df_row_limit']
    df_chunk_no=1
    column_header=set_column_header(['START_ID','END_ID'],config)

    ## generate relationship data
    for i in range(start_id, start_id+no_rels):

        list = []
        ## create relationship from random source/target IDs
        list.append(config['source_node_label']+str(random.randint(nodeidrange[config['source_node_label']]['lower'],nodeidrange[config['source_node_label']]['upper'])))
        list.append(config['target_node_label']+str(random.randint(nodeidrange[config['target_node_label']]['lower'],nodeidrange[config['target_node_label']]['upper'])))

        if "properties" in config:
            list=generate_properties(list,config['properties'])

        data.append(list)

        ## write data chunk to file if df rows exceed chunk size
        if i % df_row_limit == 0:
            write_to_file(filename,output_format,data,column_header,df_chunk_no)
            df_chunk_no+=1
            data=[]

    ## write final data to file
    write_to_file(filename,output_format,data,column_header,df_chunk_no)

    return filename
