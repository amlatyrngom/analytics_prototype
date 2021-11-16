import pandas as pd
import numpy as np
from datetime import date

l_names=["L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER",
       "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX",
       "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE",
       "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT",
       "L_SHIPMODE", "L_COMMENT"]
def q1():
    df = pd.read_csv("lineitem.tbl", sep='|', header=None, names=l_names, index_col=False)
    df["L_SHIPDATE"] = pd.to_datetime(df['L_SHIPDATE'], format='%Y-%m-%d')
    date_comp = np.datetime64('1998-09-02')
    df = df[df["L_SHIPDATE"] < date_comp]
    df = df.groupby(['L_RETURNFLAG', 'L_LINESTATUS']).agg({
        'L_QUANTITY': sum
    })
    # df = df[["L_RETURNFLAG", "L_LINESTATUS", "L_QUANTITY", "L_SHIPDATE"]]
    print(df)



q1()

