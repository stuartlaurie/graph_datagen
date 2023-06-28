from multiprocessing import Pool
from helpers.create_nodes import *
from helpers.create_rels import *
from helpers.validate_config import *

import sys
import multiprocessing
import yaml
import os
import time
import logging

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

def create_output_dir(data_dir):
    logger=logging.getLogger('datagenerator')
    logger.info("Creating output directory: " + data_dir)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    return data_dir

def create_filename(data_dir,prefix,id,output_format):
    ## Create Data files
    filename=os.path.join(data_dir, prefix+str(id)+"."+output_format)
    return filename

def nodes(work_data):
    ## work_data = i, filename, output_format, start_id, no_nodes, label, config, general_config
    create_node_data(work_data[0],work_data[1],work_data[2],work_data[3],work_data[4],work_data[5],work_data[6],work_data[8])

def rels(work_data):
    ## work_data = i, filename, output_format, start_id, no_rels, total_rels, label, config, nodelabelcount, general_config
    create_rel_data(work_data[0],work_data[1],work_data[2],work_data[3],work_data[4],work_data[5],work_data[6],work_data[7],work_data[8])

def calculate_work_split(config,records_per_file,data_dir,output_format,idrange,generalconfig):
    ## config, records_per_file, data_dir, output_format

    i=0
    if "start_id" in config:
        start_id=config['start_id']
    else:
        start_id=1
    work=[]
    filelist=[]

    if records_per_file >= config['no_to_generate']:
        filename=create_filename(data_dir,config['label'],i,output_format)
        job=[1, filename, output_format, start_id, config['no_to_generate'], config['label'], config, idrange, generalconfig]
    else:
        while start_id <= idrange[config['label']]['upper']:
            filename=create_filename(data_dir,config['label'],i,output_format)
            job=[i, filename, output_format, start_id, records_per_file, config['label'], config, idrange, generalconfig]
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
            job=[i, filename, output_format, start_id, records_per_file, config['label'], config, idrange, generalconfig]


    work.append(job)
    filelist.append(filename)

    return work, filelist

def node_pool(processes,work):
    with Pool(processes) as nodePool:
        nodePool.map(nodes, work)

def rel_pool(processes,work):
    with Pool(processes) as relPool:
        relPool.map(rels, work)

def load_config(configuration):
    global config
    global idrange
    with open(configuration) as config_file:
        config = yaml.load(config_file, yaml.SafeLoader)
        config, idrange = validate_config(config)

def main():

    logger=logging.getLogger('datagenerator')
    logger.info("**---------STARTING GENERATOR---------**")

    ## read YAML configuration
    configuration = sys.argv[1]
    load_config(configuration)

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

    logger.info("**NODE GENERATION**")

    for nodeconfig in config['nodes']:

        data_dir=create_output_dir(os.path.join(base_dir, nodeconfig['label']))
        work, node_files=calculate_work_split(nodeconfig, records_per_file, data_dir, output_format, idrange, config)
        #logger.debug(work)

        logger.info("Generating " + str(nodeconfig['no_to_generate']) + " " + nodeconfig['label'] + " in: " + str((len(work))) + " jobs")
        node_generation_start=time.time()
        node_pool(processes,work)
        node_generation_end=time.time()

        if output_format != "parquet":
            header_file=create_node_header(base_dir,nodeconfig,config['admin-import'])
            import_node_config[nodeconfig['label']]=[header_file]+[data_dir+"/.*"] ## use node_files for all filenames


    ##############################
    ## Generate Relationship files
    ##############################

    logger.info("**RELATIONSHIP GENERATION**")

    for relationshipconfig in config['relationships']:

        data_dir=create_output_dir(os.path.join(base_dir, relationshipconfig['label']))
        work, rel_files=calculate_work_split(relationshipconfig, records_per_file, data_dir, output_format, idrange, config)

        logger.info("Generating " + str(relationshipconfig['no_to_generate']) + " " + relationshipconfig['label'] + " relationships in: " + str((len(work))) + " jobs")
        rel_start=time.time()
        rel_pool(processes,work)
        rel_end=time.time()

        if output_format != "parquet":
            header_file=create_rel_header(base_dir,relationshipconfig)
            import_rel_config[relationshipconfig['label']]=[header_file]+[data_dir+"/.*"] ## rel_files for all filenames

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
