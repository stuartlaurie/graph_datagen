from helpers import *

def create_node_header(data_dir, config, admin_config):
    filename=data_dir+"/"+config['label']+"Node_Headers.csv"

    if "id_property_name" not in config:
        config['id_property_name']="id"

    if admin_config['type'] == "incremental":
        header=[config['id_property_name']+':ID('+config['label']+')'+'{label:'+config['label']+'}']
    else:
        header=[config['id_property_name']+':ID('+config['label']+')']

    filename=writeImportHeader(filename,header,config)

    return filename

def create_node_data(filename, output_format, start_id, no_nodes, label, config, generalconfig):

    ## randomly generate node data
    data = []
    df_chunk=generalconfig['df_row_limit']
    df_chunk_no=1
    column_header=setColumnHeader(['START_ID'],config)

    for i in range(start_id, start_id+no_nodes):
        list = []
        list.append(label+str(i))

        if "labels" in config:
            list=generateLabels(list,config['labels'])

        if "properties" in config:
            list=generateProperties(list,config['properties'])

        data.append(list)

        if i % df_chunk == 0:
            write_to_file(filename,output_format,data,column_header,df_chunk_no)
            df_chunk_no+=1
            data=[]

    write_to_file(filename,output_format,data,column_header,df_chunk_no)

    return filename
