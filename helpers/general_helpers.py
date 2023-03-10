import pandas as pd
from fastparquet import write

def set_column_header(header,config):
    ## creates header for dataframe based on config
    if "labels" in config:
        for labelconfig in config['labels']:
            header.append(labelconfig['name'])
    if "properties" in config:
        for propertyconfig in config['properties']:
            header.append(propertyconfig['name'])
    return header

def write_to_file(filename,output_format,data,column_header,df_chunk):
    ## write the data to file
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
