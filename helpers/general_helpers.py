import pandas as pd
import numpy as np
import time
import logging
from fastparquet import write
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

def write_to_file(filename,output_format,df,df_chunk):

    if len(df.index) > 0:
        rel_batch_write_start=time.time()
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

        rel_batch_write_end=time.time()
        logger.debug("Dataframe size: " + str(round(df.memory_usage(deep=True).sum()/(1024 * 1024 * 1024),4)) + " GB")
        logger.debug("Batch write time: " + str(df_chunk) + " - " + str(round(rel_batch_write_end - rel_batch_write_start,2)) + " seconds")
