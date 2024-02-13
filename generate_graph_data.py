from multiprocessing import Pool
from helpers.create_nodes import *
from helpers.create_rels import *
from helpers.validate_config import *

import sys
import multiprocessing
import yaml
import time
import logging
import tqdm

## global logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('graphgenerator-debug.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

def create_filename(data_dir,prefix,id,output_format):
    ## Create Data files
    filename=os.path.join(data_dir, prefix+str(id)+"."+output_format)
    return filename

def generate_data(work_data):
    ## work_data = i, filename, output_format, start_id, no_nodes, label, config, general_config
    if work_data[1]=="node":
        create_node_data(work_data[1],work_data[2],work_data[4],work_data[4],work_data[5],work_data[6],work_data[7],work_data[9],work_data[10])
    elif work_data[1]=="rel":
        create_rel_data(work_data[1],work_data[2],work_data[3],work_data[4],work_data[5],work_data[6],work_data[7],work_data[8],work_data[9],work_data[10])
    else:
        logger.debug("Not a recognized type, should be either node or rel")
        exit()

def calculate_work_split(work, type, config,records_per_file,data_dir,output_format,idrange,generalconfig,cycle):
    ## config, records_per_file, data_dir, output_format
    if cycle == 1:
        logger.debug("Creating Config for " + config['label'] + ", generating " + str(idrange[config['label']]['no_to_generate']) + ' records')

    i=0
    if "start_id" in config:
        start_id=config['start_id']
    else:
        start_id=1
    filelist=[]

    ## set filecount roughly based on no. generate, decent medium between tons of small files and slowdown accessing larger files
    filecount=round(idrange[config['label']]['upper']/records_per_file)

    if records_per_file >= idrange[config['label']]['no_to_generate']:
        filename=create_filename(data_dir,config['label'],i,output_format)
        job=[1, type, filename, output_format, start_id, idrange[config['label']]['no_to_generate'], config['label'], config, idrange, generalconfig, cycle]
    else:
        while start_id <= idrange[config['label']]['upper']:
            filename=create_filename(data_dir,config['label'],i,output_format)
            job=[i, type, filename, output_format, start_id, records_per_file, config['label'], config, idrange, generalconfig, cycle]
            work.append(job)
            filelist.append(filename)
            i=i+1
            start_id=start_id+records_per_file

        if start_id > idrange[config['label']]['upper']:
            discard=work.pop() ## get rid of incorrect entry
            i=i-1

            start_id=start_id-records_per_file
            records_per_file=idrange[config['label']]['upper']-(start_id)+1

            filename=create_filename(data_dir,config['label'],i,output_format)
            job=[i, type, filename, output_format, start_id, records_per_file, config['label'], config, idrange, generalconfig, cycle]


    work.append(job)
    filelist.append(filename)

    return work, filelist

def work_pool(processes,work):
    with Pool(processes) as work_pool:
        for _ in tqdm.tqdm(work_pool.imap(generate_data, work), total=len(work)):
            pass

def load_config(configuration):
    with open(configuration) as config_file:
        config = yaml.load(config_file, yaml.SafeLoader)
        config, idrange = validate_config(config)
    return config, idrange

def main():

    logger=logging.getLogger('datagenerator')
    logger.info("**---------STARTING GENERATOR---------**")

    ## read YAML configuration
    configuration = sys.argv[1]
    config, idrange=load_config(configuration)

    ## Get general settings
    records_per_file=config['records_per_file']
    output_format=config['output_format']
    processes=multiprocessing.cpu_count() ## config['threads']
    logger.info("Using %s processes" % processes)

    node_files=[]
    rel_files=[]

    ## create directory for output
    base_dir=create_output_dir(os.path.join(str(config['basepath']), str(config['output_dir'])))
    total_start=time.time()

    ## setup dicts
    import_node_config=dict()
    import_rel_config=dict()
    rel_config=dict()

    ##############################
    ## Generate Node files
    ##############################

    cycles=config['cycles']
    work=[]

    logger.info("**PROCESSING CONFIG**")
    logger.info("**CREATING CONFIG FOR %i SUBGRAPHS**",cycles)
    for cycle in tqdm.tqdm(range(1,cycles+1),total=cycles):

        if cycle > 1:
            ## recalculate ids per cycle
            config,idrange=update_config_ids(config,idrange)

        #logger.debug(idrange)


        ##############################
        ## Process node config
        ##############################

        if cycle == 1:
            logger.debug("**PROCESSING NODE CONFIG**")

        for nodeconfig in config['nodes']:

            data_dir=create_output_dir(os.path.join(base_dir, nodeconfig['label']))
            work, node_files=calculate_work_split(work,"node", nodeconfig, records_per_file, data_dir, output_format, idrange, config, cycle)

            if output_format != "parquet":
                header_file=create_node_header(base_dir,nodeconfig,config['admin-import'])
                import_node_config[nodeconfig['label']]=[header_file]+[data_dir+"/.*"] ## use node_files for all filenames

        ##############################
        ## Process Relationship config
        ##############################

        if cycle == 1:
            logger.debug("**PROCESSING RELATIONSHIP CONFIG**")

        for relationshipconfig in config['relationships']:

            data_dir=create_output_dir(os.path.join(base_dir, relationshipconfig['label']+"_"+relationshipconfig['source_node_label']+"_"+relationshipconfig['target_node_label']))
            work, rel_files=calculate_work_split(work,"rel", relationshipconfig, records_per_file, data_dir, output_format, idrange, config, cycle)

            if output_format != "parquet" and cycle == 1:
                header_file=create_rel_header(base_dir,relationshipconfig)
                relconfig=[header_file,data_dir+"/.*"]
                if relationshipconfig['label'] in import_rel_config:
                    import_rel_config[relationshipconfig['label']].append(relconfig) ## rel_files for all filenames
                else:
                    import_rel_config[relationshipconfig['label']]=[relconfig]

    ##############################
    ## Generate the data
    ##############################

    logger.info("**DATA GENERATION**")
    work_start=time.time()
    work_pool(processes,work)
    work_end=time.time()
    logger.info("GENERATION FINISHED in " + str(round(work_end - work_start,2)) + " seconds")


    ##############################
    ## Generate Import script
    ##############################

    if output_format != "parquet":
        create_adminimport_command(base_dir,import_node_config,import_rel_config,config['admin-import'])
        warn_about_incremental_constraint(config)

    total_end=time.time()

    logger.info("Total time: " + str(round(total_end - total_start,2)) + " seconds")

if __name__ == '__main__':
    main()
