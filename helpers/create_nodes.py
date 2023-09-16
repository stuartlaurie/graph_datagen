from helpers.general_helpers import *
from helpers.admin_import import *
from helpers.create_data import *
from helpers.write_to_file import *

logger=logging.getLogger(__name__)

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

def create_node_data(process, filename, output_format, start_id, no_nodes, label, config, generalconfig, cycle):

    logger.debug("Starting Process: " + str(process) + " generating: "+ str(no_nodes) + " nodes")

    df_row_limit=generalconfig['df_row_limit']
    df_chunk_no=1
    #column_header=set_column_header(['START_ID'],config)
    id_chunks=get_id_chunks(start_id, (start_id+no_nodes), df_row_limit)
    #logger.debug(id_chunks)

    ## split into chunks based on df_row_limit
    inner_start=time.time()

    for chunk in id_chunks:
        df = []
        id_batch_start=time.time()
        ## generate sequential ids
        #logger.debug("Generating chunk: "+str(chunk[0])+" to " + str(chunk[1]))
        df = generate_ids(df,'id',label,chunk[0],chunk[1])

        if "labels" in config:
            df=generate_labels(df,config['labels'])

        if "properties" in config:
            df=batch_generate_properties(df,config['properties'])

        #logger.debug(df.head())

        id_batch_end=time.time()
        #logger.debug("node batch generation time: " + str(round(id_batch_end - id_batch_start,2)) + " seconds")

        write_to_file(filename,output_format,df,df_chunk_no, cycle)
        df_chunk_no+=1

    inner_end=time.time()
    logger.debug("Finished Process: " + str(process) + ", generating: "+ str(no_nodes) + " nodes in " + str(round(inner_end - inner_start,2)) + " seconds")

    return filename
