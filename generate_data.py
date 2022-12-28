from multiprocessing import Pool
from writeNodes import *
from writeRels import *
from cmdlineoptions import *

import multiprocessing
import yaml
import os
import time

config = dict()

def nodes(work_data):
    ## work_data = i, filename, start_id, records_per_file, no_nodes
    inner_start=time.time()
    create_node_data(work_data[1],work_data[2],work_data[3],work_data[5],work_data[8])
    inner_end=time.time()
    print("Process: " + str(work_data[0]) + ", finished generating: "+ str(work_data[3]) + " data in " + str((inner_end - inner_start)) + " seconds", flush=True)

def rels(work_data):
    ## work_data = i, filename, start_id, records_per_file, no_nodes
    inner_start=time.time()
    create_rel_data(work_data[1],work_data[2],work_data[3],work_data[4],work_data[5],work_data[6],work_data[7],work_data[8])
    inner_end=time.time()
    print("Process: " + str(work_data[0]) + ", finished generating: "+ str(work_data[3]) + " " + " data in " + str((inner_end - inner_start)) + " seconds", flush=True)


def create_adminimport_command(nodefiles,relfiles,config):
    loadCommand=open(base_dir+'/importCommand.sh', 'w', newline='')

    loadCommand.write(config['path'] + " database import full ")
    loadCommand.write(" \\\n")

    for key, value in config['options'].items():
        loadCommand.write("  --"+key+"=" + str(value))
        loadCommand.write(" \\\n")

    for key, value in nodefiles.items():
        loadCommand.write("  --nodes="+key+"=" + ','.join(value))
        loadCommand.write(" \\\n")

    for key, value in relfiles.items():
        loadCommand.write("  --relationships="+key+"=" + ','.join(value))
        loadCommand.write(" \\\n")

    loadCommand.write(" " + config['database']+"\n")
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

def calculate_work_split(no_records,records_per_file,no_nodes,data_dir,prefix,end_id,start_label,output_format):
    no_jobs=round(no_records/records_per_file)
    if (no_jobs <= 1):
        no_jobs=1
        records_per_file=no_records

    start_id=1
    work=[]
    filelist=[]

    for i in range(1, no_jobs+1):
        filename=create_filename(data_dir,prefix,i,output_format)
        job=[i, filename, start_id, records_per_file, no_nodes, prefix, end_id, start_label, output_format]
        work.append(job)

        filelist.append(filename)
        start_id=start_id+records_per_file

    return work, filelist

def node_pool(processes):
    with Pool(processes) as customerPool:
        customerPool.map(nodes, work)

def rel_pool(processes):
    with Pool(processes) as txnPool:
        txnPool.map(rels, work)

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
    rel_config=dict()

    ##############################
    ## Generate Node files
    ##############################

    for node in config['nodes']:

        data_dir=create_output_dir(os.path.join(base_dir, node['label']))
        work, node_files=calculate_work_split(node['no_to_generate'], records_per_file, node['no_to_generate'], data_dir, node['label'], 1,"",output_format)

        print ("Generating "+ node['label'] + " in: " + str((len(work))) + " jobs")
        node_generation_start=time.time()
        node_pool(processes)
        node_generation_end=time.time()

        if output_format != "parquet":
            header_file=create_node_header(base_dir,node['label'])
            import_node_config[node['label']]=[header_file]+node_files


    ##############################
    ## Generate Relationship files
    ##############################


    for relationship in config['relationships']:

        data_dir=create_output_dir(os.path.join(base_dir, relationship['label']))
        work,rel_files=calculate_work_split(relationship['no_to_generate'], records_per_file, relationship['no_to_generate'], data_dir, relationship['label'], node['no_to_generate'], relationship['source_node_label'] ,output_format)

        print ("Creating " + relationship['label'] + " relationships in: " + str((len(work))) + " jobs")
        txn_start=time.time()
        rel_pool(processes)
        txn_end=time.time()

        if output_format != "parquet":
            header_file=create_rel_header(base_dir,relationship['label'])
            import_rel_config[relationship['label']]=[header_file]+rel_files

    ##############################
    ## Generate Import script
    ##############################

    if output_format != "parquet":
        create_adminimport_command(import_node_config,import_rel_config,config['admin-import'])

    total_end=time.time()

    print("Total time: " + str((total_end - total_start)) + " seconds")
