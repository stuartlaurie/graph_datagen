import csv
import numpy as np
import datetime
from faker import Faker
fake = Faker()

def generateProperties(row, properties):
    for property in properties:
        if property['type'] == 'int':
            row.append(np.random.randint(property['lower'],property['upper']))
        elif property['type'] == 'list':
            row.append(np.random.choice(property['values']))
        elif property['type'] == 'date':
            start_date=datetime.date(year=property['lower']['year'], month=property['lower']['month'], day=property['lower']['day'])
            end_date=datetime.date(year=property['upper']['year'], month=property['upper']['month'], day=property['upper']['day'])
            row.append(fake.date_between(start_date=start_date, end_date=end_date))
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
        else:
            print("WARNING: Unknown property type, ignoring.. " + property['type'])
            row.append("")
    return row

def writeImportHeader(filename,header,config):
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
    if "properties" in config:
        for propertyconfig in config['properties']:
            header.append(propertyconfig['name'])
    return header
