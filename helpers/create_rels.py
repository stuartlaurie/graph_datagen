import random
import time
from helpers.general_helpers import *
from helpers.admin_import import *
from helpers.create_data import *
from helpers.write_to_file import *

logger=logging.getLogger(__name__)

def create_rel_header(data_dir, config):
    ## create the relationship header file
    filename=data_dir+"/"+config['label']+"_"+config['source_node_label']+"_"+config['target_node_label']+"_Rel_Headers.csv"

    header=[
        # ':IGNORE',
        ':START_ID('+config['source_node_label']+')',
        ':END_ID('+config['target_node_label']+')',
    ]
    filename=write_import_header(filename,header,config)

    return filename


def create_rel_data(process,filename, output_format, start_id, no_rels, label, config, nodeidrange, generalconfig,cycle):

    if cycle == 1:
        logger.debug("Starting Process: " + str(process) + ", generating: "+ str(no_rels) + " rels")

    df_row_limit=generalconfig['df_row_limit']
    df_chunk_no=1
    column_header=set_column_header(['START_ID','END_ID'],"")
    id_chunks=get_id_chunks(start_id, (start_id+no_rels), df_row_limit)
    #logger.debug(id_chunks)

    inner_start=time.time()

    for chunk in id_chunks:
        df = []
        data = []
        id_batch_start=time.time()
        ## generate sequential ids
        if "rel_multiplier" in config:
            logger.debug("relationship multiplier selected")
            for i in range(chunk[0],chunk[1]):

                ## create relationship from random source/target IDs
                source_node=config['source_node_label']+str(random.randint(nodeidrange[config['source_node_label']]['lower'],nodeidrange[config['source_node_label']]['upper']))
                target_node=config['target_node_label']+str(random.randint(nodeidrange[config['target_node_label']]['lower'],nodeidrange[config['target_node_label']]['upper']))

                rel_multiplier=random.randint(config['rel_multiplier']['lower'],config['rel_multiplier']['upper'])
                for j in range (0,rel_multiplier):
                    list = []
                    list.append(source_node)
                    list.append(target_node)
                    data.append(list)
            df = pd.DataFrame(data, columns=column_header)

        else:
            #logger.debug("Generating chunk: "+str(chunk[0])+" to " + str(chunk[1]))
            df = generate_ids(df,'id',label,chunk[0],chunk[1])
            df = generate_random_ids(df,'sourceid',config['source_node_label'],nodeidrange[config['source_node_label']]['lower'],nodeidrange[config['source_node_label']]['upper'],chunk[1]-chunk[0])
            df = generate_random_ids(df,'targetid',config['target_node_label'],nodeidrange[config['target_node_label']]['lower'],nodeidrange[config['target_node_label']]['upper'],chunk[1]-chunk[0])

        if "properties" in config:
            rel_batch_generate_start=time.time()
            df = batch_generate_properties(df,config['properties'])
            rel_batch_generate_end=time.time()
            #logger.debug("rel batch generation time: " + str(round(rel_batch_generate_end - rel_batch_generate_start,2)) + " seconds")

        #logger.debug(df.head())
        if 'id' in df:
            df.drop('id', axis=1, inplace=True)
        #logger.debug(df.head())
        write_to_file(filename,output_format,df,df_chunk_no,cycle)
        df_chunk_no+=1

    inner_end=time.time()

    if cycle == 1:
        logger.debug("Finished Process: " + str(process) + ", generating: "+ str(no_rels) + " rels in " + str(round(inner_end - inner_start,2)) + " seconds")

    return filename
