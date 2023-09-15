import logging
from more_itertools import pairwise

logger=logging.getLogger(__name__)

def set_column_header(header,config):
    ## creates header for dataframe based on config
    if "labels" in config:
        for labelconfig in config['labels']:
            header.append(labelconfig['name'])
    if "properties" in config:
        for propertyconfig in config['properties']:
            if propertyconfig['type'] != 'int' and propertyconfig['type'] != 'list':
                header.append(propertyconfig['name'])
    return header

def get_id_chunks(start_id,end_id,chunksize):

    ranges=list(range(start_id,end_id,chunksize))
    ranges.append(end_id)
    splits=list(pairwise(ranges))

    return splits
