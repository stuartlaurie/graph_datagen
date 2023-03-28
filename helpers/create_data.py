import datetime
import numpy as np
from faker import Faker
fake = Faker()

def generate_properties(row, properties):
    ## add properties based on config definition
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
        elif property['type'] == 'array':
            array=np.random.randint(low=property['lower'], high=property['upper'], size=property['size'])
            separator=";"
            row.append(separator.join(str(e) for e in array))
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

def generate_labels(row, labels):
    ## add labels based on config definition
    for label in labels:
        if "probability" in label:
            row.append(np.random.choice(label['values'],p=label['probability']))
        else:
            row.append(np.random.choice(label['values']))
    return row
