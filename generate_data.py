from multiprocessing import Pool
from writeNodes import *
from writeRels import *
from cmdlineoptions import *

import os
import time

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


def create_load_command(nodefiles,relfiles,database):
    loadCommand=open(base_dir+'/importCommand.sh', 'w', newline='')

    cmd_string="../neo4j-enterprise-4.3.6/bin/neo4j-admin import"

    for key, value in nodefiles.items():
        cmd_string = cmd_string + " --nodes="+key+"=" + ','.join(value)

    for key, value in relfiles.items():
        cmd_string = cmd_string + " --relationships="+key+"=" + ','.join(value)

    cmd_string=cmd_string + " --ignore-extra-columns=true --skip-bad-relationships=true --skip-duplicate-nodes=true --high-io=true --database="+ database+ " "
    loadCommand.write(cmd_string+"\n")
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

def tar_files(data_dir,label,files):
    filename = os.path.join(base_dir, label+".tar")
    tar_cmd="tar -cvf " + filename + " " + os.path.join(base_dir, label)
    os.system(tar_cmd)

    ## remove files and directory
    for file in files:
        try:
            os.remove(file)
        except OSError as error:
            print(error)
            print("File path can not be removed")

    os.rmdir(data_dir)
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
        if (i==no_jobs):
            records_per_file=no_records-start_id
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

if __name__ == '__main__':

    database, no_nodes, no_rels, records_per_file, processes, output_format = commandlineOptions(sys.argv[0],sys.argv[1:])
    node_files=[]
    rel_files=[]

    ## create directory for output
    base_dir=create_output_dir("DATA_"+str(no_nodes))
    total_start=time.time()

    ## setup dicts
    import_node_config=dict()
    import_rel_config=dict()
    rel_config=dict()

    rel_config["FOLLOWS"]=no_rels

    ##############################
    ## Generate Node files
    ##############################

    node_label = "User"
    no_nodes = no_nodes

    data_dir=create_output_dir(os.path.join(base_dir, node_label))
    header_file=create_node_header(base_dir)

    ## User
    work, node_files=calculate_work_split(no_nodes, records_per_file, no_nodes, data_dir, node_label, 1,"",output_format)

    print ("Generating "+ node_label + " in: " + str((len(work))) + " jobs")
    node_generation_start=time.time()
    node_pool(processes)
    node_generation_end=time.time()

    if output_format != "parquet":
        print ("Creating .tar file ..")
        tar_file=tar_files(data_dir,node_label,node_files)

        import_node_config[node_label]=[header_file]+[tar_file]

    ##############################
    ## Generate Relationship files
    ##############################


    for rel_label, no_rels in rel_config.items():

        data_dir=create_output_dir(os.path.join(base_dir, rel_label))
        header_file=create_rel_header(base_dir, rel_label)

        work,rel_files=calculate_work_split(no_rels, records_per_file, no_rels, data_dir, rel_label, no_nodes, node_label,output_format)

        print ("Creating " + rel_label + " relationships in: " + str((len(work))) + " jobs")
        txn_start=time.time()
        rel_pool(processes)
        txn_end=time.time()


    if output_format != "parquet":
        print ("Creating .tar file ..")
        tar_file=tar_files(data_dir,rel_label,rel_files)
        import_rel_config[rel_label]=[header_file]+[tar_file]

        ##############################
        ## Generate Import script
        ##############################

        create_load_command(import_node_config,import_rel_config,database)

    total_end=time.time()

    print("Total time: " + str((total_end - total_start)) + " seconds")
