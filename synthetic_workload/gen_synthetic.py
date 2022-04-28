import numpy as np
import pandas as pd


# Constants used with table
str_values = ["aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh"]
float_values = [0.37, 0.73, 3.7, 7.3, 0.037, 0.073, 0.0037, 0.0073]
date_values = ["1996-01-02", "1996-12-01", "1993-10-14", "1995-10-11", "1994-07-30", "1992-02-21", "1996-01-10", "1995-07-16"]


log_A_size = 20
A_size = 2**log_A_size
B_size = 2**(log_A_size + 3)

def gen_A():
    id = np.arange(A_size, dtype=np.int)
    filter_col = [i%512 for i in id]
    df = pd.DataFrame({
        'id': np.array(id).astype(np.int),
        'filter_col': np.array(filter_col).astype(np.int),
    })
    print(df.head())
    df.to_csv("A.csv", index=False, sep=',', header=False)

def gen_B():
    id = np.arange(B_size, dtype=np.int)
    fk = [i%A_size for i in id]
    df = pd.DataFrame({
        'id': np.array(id).astype(np.int),
        'fk': np.array(fk).astype(np.int),
    })
    print(df.head())
    df.to_csv("B.csv", index=False, sep=',', header=False)

gen_A()
gen_B()
