import csv

def create_adminimport_command(base_dir,nodefiles,relfiles,config, schema_file):
    ## create a sh script that will run admin-import and any other cypher
    loadCommand=open(base_dir+'/importCommand.sh', 'w', newline='')

    ## cypher to drop db
    if 'neo4j' in config.keys() and config['neo4j']['recreate_db'] == True:
        loadCommand.write("echo \"DROP DATABASE "+ config['database'] + "\" | " + config['path'] + "cypher-shell -u " + config['neo4j']['username'] + " -p " + config['neo4j']['password'] + "\n")

    ## admin import command
    loadCommand.write(config['path'] + config['cmd'] + " " +config['type']+ " " + config['database'])
    loadCommand.write(" \\\n")

    for key, value in config['options'].items():
        loadCommand.write("  --"+key+"=" + str(value) + " \\\n")

    for key, value in nodefiles.items():
        loadCommand.write("  --nodes="+key+"=" + ','.join(value) + " \\\n")

    for key, value in relfiles.items():
        for item in relfiles[key]:
            loadCommand.write("  --relationships="+key+"=" + item[0]+','+item[1] + " \\\n")

    loadCommand.write("  --schema="+schema_file+"\\\n")

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
                if propertyconfig['type'] == 'array':
                    header.append(propertyconfig['name']+":int[]")
                else:
                    header.append(propertyconfig['name']+":"+propertyconfig['type'])

    with open(filename, 'w', newline='') as headerfile:
        csv_writer = csv.writer(headerfile)
        csv_writer.writerow(header)

    return filename


def create_schema(config):
    
    filename=config['schema_file']
    print ("Cypher for schema written to: "+filename)
    
    ## create constraints for node IDs
    with open(filename, 'w') as schemafile:
        for nodeconfig in config['nodes']:
            create_constraint(nodeconfig['label'], nodeconfig['id_property_name'], schemafile)
                        
            if 'properties' in nodeconfig:
                for propertyconfig in nodeconfig['properties']:
                    if 'index' in propertyconfig:
                        create_index(nodeconfig['label'], propertyconfig['name'], propertyconfig['index'], schemafile)


def create_constraint(label, property, schemafile):
    cypher="\nCREATE CONSTRAINT "+ label+"Constraint"
    cypher+=" IF NOT EXISTS"
    cypher+="\nFOR (n:" + label+")"
    cypher+="\nREQUIRE n."+ property+" IS UNIQUE;\n"
    schemafile.write(cypher)

def create_index(label, property, type, schemafile):
    if type not in ['RANGE', 'LOOKUP', 'POINT', 'TEXT', 'FULL-TEXT', 'VECTOR']:
        type='RANGE'
    cypher="\nCREATE "+type+" INDEX "+ label+"Index"
    cypher+=" IF NOT EXISTS"
    cypher+="\nFOR (n:" + label+")"
    cypher+="\nON n."+ property+";\n"
    schemafile.write(cypher)