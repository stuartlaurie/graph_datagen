import csv

def create_adminimport_command(base_dir,nodefiles,relfiles,config):
    ## create a sh script that will run admin-import and any other cypher
    loadCommand=open(base_dir+'/importCommand.sh', 'w', newline='')

    ## cypher to drop db
    if 'neo4j' in config.keys() and config['neo4j']['recreate_db'] == True:
        loadCommand.write("echo \"DROP DATABASE "+ config['database'] + "\" | " + config['path'] + "cypher-shell -u " + config['neo4j']['username'] + " -p " + config['neo4j']['password'] + "\n")

    ## admin import command
    loadCommand.write(config['path'] + config['cmd'] + " " +config['type'])
    loadCommand.write(" \\\n")

    for key, value in config['options'].items():
        loadCommand.write("  --"+key+"=" + str(value) + " \\\n")

    for key, value in nodefiles.items():
        loadCommand.write("  --nodes="+key+"=" + ','.join(value) + " \\\n")

    for key, value in relfiles.items():
        loadCommand.write("  --relationships="+key+"=" + ','.join(value) + " \\\n")

    loadCommand.write(" " + config['database']+ " \n")

    ## cypher to create/register database in neo4j
    if 'neo4j' in config.keys() and config['neo4j']['recreate_db'] == True:
        loadCommand.write("echo \"CREATE DATABASE "+ config['database'] + "\" | " + config['path'] + "cypher-shell -u " + config['neo4j']['username'] + " -p " + config['neo4j']['password'] + "\n")

    loadCommand.close()


def write_import_header(filename,header,config):
    ## write the admin-import header file
    if "labels" in config:
        for labelconfig in config['labels']:
            header.append(":LABEL")

    if "properties" in config:
        for propertyconfig in config['properties']:
            if 'output_type' in propertyconfig:
                header.append(propertyconfig['name']+":"+propertyconfig['output_type'])
            else:
                header.append(propertyconfig['name']+":"+propertyconfig['type'])

    with open(filename, 'w', newline='') as headerfile:
        csv_writer = csv.writer(headerfile)
        csv_writer.writerow(header)

    return filename


def warn_about_incremental_constraint(config):
    ## when running incremental need to make sure constraints are set
    ## this generates the cypher to use
    if config['admin-import']['type'] == "incremental":

        filename=config['output_dir']+"/Neo4jConstraints.cypher"
        print ("WARNING: Don't forget you'll need constraints for incremental to work !!!")
        print ("Cypher for constraints written to: "+filename)

        with open(filename, 'w') as constraintsfile:
            for nodeconfig in config['nodes']:
                print ("\nCREATE CONSTRAINT "+ nodeconfig['label']+"Constraint", file=constraintsfile)
                print ("FOR (n:" + nodeconfig['label']+")", file=constraintsfile)
                print ("REQUIRE n."+ nodeconfig['id_property_name']+" IS UNIQUE;\n", file=constraintsfile)
