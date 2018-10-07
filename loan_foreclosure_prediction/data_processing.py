from settings import *
import os
import pandas as pd


def concatenate(prefix, columns):
    prefix = prefix.lower()
    files = os.listdir(RAW_DATA_PATH)
    full_df = []
    for file in files:
        if prefix in file.lower():
            df = pd.read_csv(RAW_DATA_PATH + file, '|', names=HEADER[prefix], header=None, index_col=False)
            if prefix.lower() == 'performance':
                df.drop_duplicates(subset=['id'], keep='last', inplace=True)
            df = df.loc[:, columns]
            full_df.append(df)

    full_df = pd.concat(full_df, axis=0)


    full_df.to_csv(PROCESSED_DATA_PATH + prefix + '_processed.txt', sep="|", index=False)
