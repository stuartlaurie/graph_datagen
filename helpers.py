import csv
import numpy as np
import pandas as pd
from fastparquet import write

import datetime
from faker import Faker
fake = Faker()

def generateProperties(row, properties):
    for property in properties:
        if property['type'] == 'int' or property['type'] == 'long' :
            row.append(np.random.randint(property['lower'],property['upper']))
        elif property['type'] == 'float' or property['type'] == 'double':
            precision='%.2f'
            if "precision" in property:
                precision='%.'+str(property['precision'])+'f'
            row.append(precision % np.random.uniform(low=property['lower'], high=property['upper'], size=None))
        elif property['type'] == 'list':
            if "probability" in property:
                row.append(np.random.choice(property['values'],p=property['probability']))
            else:
                row.append(np.random.choice(property['values']))
        elif property['type'] == 'date':
            start_date=datetime.datetime(year=property['lower']['year'], month=property['lower']['month'], day=property['lower']['day'])
            end_date=datetime.datetime(year=property['upper']['year'], month=property['upper']['month'], day=property['upper']['day'])
            row.append(fake.date_between(start_date=start_date, end_date=end_date))
        elif property['type'] == 'datetime' or property['type'] == 'epoch':
            start_date=datetime.datetime(year=property['lower']['year'], month=property['lower']['month'], day=property['lower']['day'], hour=property['lower']['hour'], minute=property['lower']['minute'], second=property['lower']['second'])
            end_date=datetime.datetime(year=property['upper']['year'], month=property['upper']['month'], day=property['upper']['day'], hour=property['upper']['hour'], minute=property['upper']['minute'], second=property['upper']['second'])
            generated_datetime=fake.date_time_between(start_date=start_date, end_date=end_date)
            if property['type'] == 'epoch':
                row.append(generated_datetime.strftime('%s'))
            else:
                row.append(str(generated_datetime).replace(" ", "T" ))
        elif property['type'] == 'name':
            row.append(fake.name())
        elif property['type'] == 'phone':
            row.append(fake.phone_number())
        elif property['type'] == 'email':
            row.append(fake.company_email())
        elif property['type'] == 'ssn':
            row.append(fake.ssn())
        elif property['type'] == 'ip':
            row.append(fake.ipv4())
        elif property['type'] == 'ID':
            row.append(row[0])
        else:
            print("WARNING: Unknown property type, ignoring.. " + property['type'])
            row.append("")
    return row

def add_id_range(idrange,label,start_id,no_to_generate):
    idrange[label]={}
    idrange[label]['lower']=start_id
    idrange[label]['upper']=no_to_generate+start_id-1
    return idrange

def generateLabels(row, labels):
    for label in labels:
        if "probability" in label:
            row.append(np.random.choice(label['values'],p=label['probability']))
        else:
            row.append(np.random.choice(label['values']))
    return row

def string_to_int(thing):
    if type(thing) != int:
        thing = int(thing.replace(',',''))
    return thing

def writeImportHeader(filename,header,config):
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

def setColumnHeader(header,config):
    if "labels" in config:
        for labelconfig in config['labels']:
            header.append(labelconfig['name'])
    if "properties" in config:
        for propertyconfig in config['properties']:
            header.append(propertyconfig['name'])
    return header

def write_to_file(filename,output_format,data,column_header,df_chunk):
    df = pd.DataFrame(data, columns=column_header)
    # print("Dataframe size: " + str(df.memory_usage(deep=True).sum()/(1024 * 1024 * 1024)) + " GB")
    
    if (output_format == "parquet"):
        if df_chunk == 1:
            write(filename, df)
        else:
            write(filename, df, append=True)
    elif (output_format == "gzip"):
        if df_chunk == 1:
            df.to_csv(filename, mode="w", index=False, header=False, compression="gzip")
        else:
            df.to_csv(filename, mode="a", index=False, header=False, compression="gzip")
    else:
        if df_chunk == 1:
            df.to_csv(filename, mode="w", index=False, header=False)
        else:
            df.to_csv(filename, mode="a", index=False, header=False)

def warn_about_incremental_constraint(config):
    if config['admin-import']['type'] == "incremental":

        filename=config['output_dir']+"/Neo4jConstraints.cypher"
        print ("WARNING: Don't forget you'll need constraints for incremental to work !!!")
        print ("Cypher for constraints written to: "+filename)

        with open(filename, 'w') as constraintsfile:
            for nodeconfig in config['nodes']:
                print ("\nCREATE CONSTRAINT "+ nodeconfig['label']+"Constraint", file=constraintsfile)
                print ("FOR (n:" + nodeconfig['label']+")", file=constraintsfile)
                print ("REQUIRE n."+ nodeconfig['id_property_name']+" IS UNIQUE;\n", file=constraintsfile)
