import pandas as pd
import numpy as np
import os
import time
import logging
from fastparquet import write

logger=logging.getLogger(__name__)

def create_output_dir(data_dir):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        logger.debug("Creating output directory: " + data_dir)
    return data_dir

def write_to_file(filename,output_format,df,df_chunk,cycle):

    if len(df.index) > 0:
        batch_write_start=time.time()
        if (output_format == "parquet"):
            if df_chunk == 1 and cycle == 1:
                write(filename, df)
            else:
                write(filename, df, append=True)
        elif (output_format == "gzip"):
            if df_chunk == 1 and cycle == 1:
                df.to_csv(filename, mode="w", index=False, header=False, compression="gzip")
            else:
                df.to_csv(filename, mode="a", index=False, header=False, compression="gzip")
        else:
            if df_chunk == 1 and cycle == 1:
                df.to_csv(filename, mode="w", index=False, header=False)
            else:
                df.to_csv(filename, mode="a", index=False, header=False)

        batch_write_end=time.time()
        #logger.debug("Dataframe size: " + str(round(df.memory_usage(deep=True).sum()/(1024 * 1024 * 1024),4)) + " GB")
        #logger.debug("Batch write time: " + str(df_chunk) + " - " + str(round(batch_write_end - batch_write_start,2)) + " seconds")
