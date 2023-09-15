import datetime
import numpy as np
import pandas as pd
import logging
from faker import Faker
fake = Faker()

logger=logging.getLogger(__name__)

def generate_ids(df,column_name,label,start_id,end_id):
    ids=np.arange(start_id,end_id)
    df = pd.DataFrame(ids,columns=[column_name])
    df[column_name] = label + df[column_name].map(str)
    return df

def generate_random_ids(df,column_name,label,lower,upper,size):
    df[column_name] = np.random.randint(low=lower, high=upper, size=size)
    df[column_name] = label + df[column_name].map(str)
    return df

def generate_random_id(df,label,lower,upper):
    id = label+str(np.random.randint(low=lower, high=upper))
    return id

def set_dates(property):
    start_date=datetime.datetime(year=property['lower']['year'], month=property['lower']['month'], day=property['lower']['day'])
    end_date=datetime.datetime(year=property['upper']['year'], month=property['upper']['month'], day=property['upper']['day'])
    return start_date, end_date

def batch_generate_properties(df, properties):
    for property in properties:
        if property['type'] == 'int' or property['type'] == 'long' :
            df[property['name']] = np.random.randint(low=property['lower'], high=property['upper'], size=len(df))
        elif property['type'] == 'float' or property['type'] == 'double':
            precision='%.2f'
            if "precision" in property:
                precision='%.'+str(property['precision'])+'f'
            df[property['name']] = [precision % np.random.uniform(low=property['lower'], high=property['upper']) for i in range(len(df))]
        elif property['type'] == 'list':
            if "probability" in property:
                df[property['name']] = np.random.choice(property['values'],p=property['probability'], size=len(df))
            else:
                df[property['name']] = np.random.choice(property['values'], size=len(df))
        elif property['type'] == 'array':
            separator=";"
            df[property['name']]=[separator.join(str(e) for e in np.random.randint(low=property['lower'], high=property['upper'], size=property['size'])) for i in range(len(df))]
        elif property['type'] == 'boolean':
            df[property['name']] = np.random.choice(["true","false"], size=len(df))
        elif property['type'] == 'date':
            start_date, end_date=set_dates(property)
            df[property['name']]=[fake.date_between(start_date=start_date, end_date=end_date) for i in range(len(df))]
        elif property['type'] == 'datetime':
            start_date, end_date=set_dates(property)
            df[property['name']]=[fake.date_time_between(start_date=start_date, end_date=end_date).strftime("%Y-%m-%dT%H:%M:%S") for i in range(len(df))]
        elif property['type'] == 'epoch':
            start_date, end_date=set_dates(property)
            df[property['name']]=[fake.date_time_between(start_date=start_date, end_date=end_date).strftime('%s') for i in range(len(df))]
        elif property['type'] == 'name':
            df[property['name']] = [fake.name() for i in range(len(df))]
        elif property['type'] == 'phone':
            df[property['name']] = [fake.phone_number() for i in range(len(df))]
        elif property['type'] == 'email':
            df[property['name']] = [fake.company_email() for i in range(len(df))]
        elif property['type'] == 'ssn':
            df[property['name']] = [fake.ssn() for i in range(len(df))]
        elif property['type'] == 'ip':
            df[property['name']] = [fake.ipv4() for i in range(len(df))]
        elif property['type'] == 'ID':
            df[property['name']] = df[0]
        else:
            logger.info("WARNING: Unknown property type, ignoring.. " + property['type'])
            ## create empty column to allow header column order
            df[property['name']]=None
    return df


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
        elif property['type'] == 'boolean':
            row.append(np.random.choice(["true","false"]))            
        elif property['type'] == 'date':
            start_date, end_date=set_dates(property)
            row.append(fake.date_between(start_date=start_date, end_date=end_date))
        elif property['type'] == 'datetime':
            start_date, end_date=set_dates(property)
            row.append(fake.date_time_between(start_date=start_date, end_date=end_date).strftime("%Y-%m-%dT%H:%M:%S"))
        elif property['type'] == 'epoch':
            start_date, end_date=set_dates(property)
            row.append(fake.date_time_between(start_date=start_date, end_date=end_date).strftime('%s'))
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
            logger.debug("WARNING: Unknown property type, ignoring.. " + property['type'])
            row.append("")
    return row

def generate_labels(df, labels):
    ## add labels based on config definition
    for label in labels:
        if "probability" in label:
            df[label['name']] = np.random.choice(label['values'],p=label['probability'], size=len(df))
        else:
            df[label['name']] = np.random.choice(label['values'], size=len(df))
    return df
