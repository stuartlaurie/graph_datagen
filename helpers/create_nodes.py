from helpers.general_helpers import *
from helpers.admin_import import *
from helpers.create_data import *

def create_node_header(data_dir, config, admin_config):
    filename=data_dir+"/"+config['label']+"_Node_Headers.csv"

    if "id_property_name" not in config:
        config['id_property_name']="id"

    if admin_config['type'] == "incremental":
        header=[config['id_property_name']+':ID('+config['label']+')'+'{label:'+config['label']+'}']
    else:
        header=[config['id_property_name']+':ID('+config['label']+')']

    filename=write_import_header(filename,header,config)

    return filename

def create_node_data(filename, output_format, start_id, no_nodes, label, config, generalconfig):

    data = []
    df_row_limit=generalconfig['df_row_limit']
    df_chunk_no=1
    column_header=set_column_header(['START_ID'],config)

    ## generate node data
    for i in range(start_id, start_id+no_nodes):
        list = []
        ## create node id
        ## TODO: allow patterns for node IDs
        list.append(label+str(i))

        ## create additional labels as defined in config
        if "labels" in config:
            list=generate_labels(list,config['labels'])

        ## create properties as defined by config
        if "properties" in config:
            list=generate_properties(list,config['properties'])

        data.append(list)

        ## write data chunk to file if df rows exceed chunk size
        if i % df_row_limit == 0:
            df = batch_generate_properties(data,column_header,config['properties'])
            write_to_file(filename,output_format,df,df_chunk_no)
            df_chunk_no+=1
            data=[]
            df=[]

    ## write final data to file
    df = batch_generate_properties(data,column_header,config['properties'])
    write_to_file(filename,output_format,df,df_chunk_no)

    return filename
