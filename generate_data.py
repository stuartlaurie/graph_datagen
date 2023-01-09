from multiprocessing import Pool
from writeNodes import *
from writeRels import *

import sys
import multiprocessing
import yaml
import os
import time

config = dict()

def nodes(work_data):
    ## work_data = i, filename, output_format, start_id, no_nodes, label
    inner_start=time.time()
    create_node_data(work_data[1],work_data[2],work_data[3],work_data[4],work_data[5])
    inner_end=time.time()
    print("Process: " + str(work_data[0]) + ", finished generating: "+ str(work_data[4]) + " data in " + str((inner_end - inner_start)) + " seconds", flush=True)

def rels(work_data):
    ## work_data = i, filename, output_format, start_id, no_rels, total_rels, label, config, nodelabelcount
    inner_start=time.time()
    create_rel_data(work_data[1],work_data[2],work_data[3],work_data[4],work_data[5],work_data[6],work_data[7])
    inner_end=time.time()
    print("Process: " + str(work_data[0]) + ", finished generating: "+ str(work_data[4]) + " " + " data in " + str((inner_end - inner_start)) + " seconds", flush=True)


def create_adminimport_command(nodefiles,relfiles,config):
    loadCommand=open(base_dir+'/importCommand.sh', 'w', newline='')

    if 'neo4j' in config.keys() and config['neo4j']['recreate_db'] == True:
        loadCommand.write("echo \"DROP DATABASE "+ config['database'] + "\" | " + config['path'] + "cypher-shell -u " + config['neo4j']['username'] + " -p " + config['neo4j']['password'] + "\n")

    loadCommand.write(config['path'] + config['cmd'])
    loadCommand.write(" \\\n")

    for key, value in config['options'].items():
        loadCommand.write("  --"+key+"=" + str(value) + " \\\n")

    for key, value in nodefiles.items():
        loadCommand.write("  --nodes="+key+"=" + ','.join(value) + " \\\n")

    for key, value in relfiles.items():
        loadCommand.write("  --relationships="+key+"=" + ','.join(value) + " \\\n")

    loadCommand.write(" " + config['database']+ " \n")

    if 'neo4j' in config.keys() and config['neo4j']['recreate_db'] == True:
        loadCommand.write("echo \"CREATE DATABASE "+ config['database'] + "\" | " + config['path'] + "cypher-shell -u " + config['neo4j']['username'] + " -p " + config['neo4j']['password'] + "\n")

    loadCommand.close()

def create_output_dir(data_dir):
    print("Creating output directory: " + data_dir)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    return data_dir

def create_filename(data_dir,prefix,id,output_format):
    ## Create Data files
    filename=os.path.join("./"+data_dir, prefix+str(id)+"."+output_format)
    return filename

def calculate_work_split(config,records_per_file,data_dir,output_format,nodelabelcount):
    ## config, records_per_file, data_dir, output_format

    i=0
    start_id=1
    work=[]
    filelist=[]

    if records_per_file >= config['no_to_generate']:
        filename=create_filename(data_dir,config['label'],i,output_format)
        job=[1, filename, output_format, start_id, config['no_to_generate'], config['label'], config, nodelabelcount]
    else:
        while start_id <= config['no_to_generate']:
            filename=create_filename(data_dir,config['label'],i,output_format)
            job=[i, filename, output_format, start_id, records_per_file, config['label'], config, nodelabelcount]
            work.append(job)
            filelist.append(filename)
            i=i+1
            start_id=start_id+records_per_file

        if start_id > config['no_to_generate']:
            discard=work.pop() ## get rid of incorrect entry
            i=i-1

            start_id=start_id-records_per_file
            records_per_file=config['no_to_generate']-(start_id)+1

            filename=create_filename(data_dir,config['label'],i,output_format)
            job=[i, filename, output_format, start_id, records_per_file, config['label'], config, nodelabelcount]


    work.append(job)
    filelist.append(filename)

    return work, filelist

def node_pool(processes):
    with Pool(processes) as nodePool:
        nodePool.map(nodes, work)

def rel_pool(processes):
    with Pool(processes) as relPool:
        relPool.map(rels, work)

def load_config(configuration):
    global config
    with open(configuration) as config_file:
        config = yaml.load(config_file, yaml.SafeLoader)

if __name__ == '__main__':

    ## read YAML configuration
    configuration = sys.argv[1]
    load_config(configuration)

    ## Get general settings
    records_per_file=config['records_per_file']
    output_format=config['output_format']
    processes=multiprocessing.cpu_count() ## config['threads']

    node_files=[]
    rel_files=[]

    ## create directory for output
    base_dir=create_output_dir(str(config['output_dir']))
    total_start=time.time()

    ## setup dicts
    import_node_config=dict()
    import_rel_config=dict()
    nodelabelcount=dict()
    rel_config=dict()

    ##############################
    ## Generate Node files
    ##############################

    for nodeconfig in config['nodes']:

        nodelabelcount[nodeconfig['label']]=nodeconfig['no_to_generate']
        data_dir=create_output_dir(os.path.join(base_dir, nodeconfig['label']))
        work, node_files=calculate_work_split(nodeconfig, records_per_file, data_dir, output_format, nodelabelcount)


        print ("Generating " + str(nodeconfig['no_to_generate']) + " " + nodeconfig['label'] + " in: " + str((len(work))) + " jobs")
        node_generation_start=time.time()
        node_pool(processes)
        node_generation_end=time.time()

        if output_format != "parquet":
            header_file=create_node_header(base_dir,nodeconfig['label'])
            import_node_config[nodeconfig['label']]=[header_file]+[data_dir+"/.*"] ## use node_files for all filenames


    ##############################
    ## Generate Relationship files
    ##############################


    for relationshipconfig in config['relationships']:

        data_dir=create_output_dir(os.path.join(base_dir, relationshipconfig['label']))
        work, rel_files=calculate_work_split(relationshipconfig, records_per_file, data_dir, output_format, nodelabelcount)

        print ("Generating " + str(relationshipconfig['no_to_generate']) + " " + relationshipconfig['label'] + " relationships in: " + str((len(work))) + " jobs")
        rel_start=time.time()
        rel_pool(processes)
        rel_end=time.time()

        if output_format != "parquet":
            header_file=create_rel_header(base_dir,relationshipconfig)
            import_rel_config[relationshipconfig['label']]=[header_file]+[data_dir+"/.*"] ## rel_files for all filenames

    ##############################
    ## Generate Import script
    ##############################

    if output_format != "parquet":
        create_adminimport_command(import_node_config,import_rel_config,config['admin-import'])

    total_end=time.time()

    print("Total time: " + str((total_end - total_start)) + " seconds")
