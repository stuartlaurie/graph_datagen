import csv

def writeImportHeader(filename,header,config):
    if "properties" in config:
        for propertyconfig in config['properties']:
            if propertyconfig['type'] == 'list':
                ## actually a string
                header.append(propertyconfig['name'])
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
