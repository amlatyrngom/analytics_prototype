import numpy as np
import pandas as pd


# Constants used with table
str_values = ["aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh"]
float_values = [0.37, 0.73, 3.7, 7.3, 0.037, 0.073, 0.0037, 0.0073]
date_values = ["1996-01-02", "1996-12-01", "1993-10-14", "1995-10-11", "1994-07-30", "1992-02-21", "1996-01-10", "1995-07-16"]


log_num_central = 12
num_central = 2**log_num_central
num_dim1 = 2**(log_num_central / 4)
num_dim2 = 2**(log_num_central - 5)
num_dim3 = 2**(log_num_central + 1)
num_dim4 = 2**(log_num_central + 2)

def gen_central():
    id = np.arange(num_central)
    id1 = id % num_dim1 + 10000 # +10000 is just to distinguish fk from pk.
    id2 = id % num_dim2 + 10000
    int32_col = id
    df = pd.DataFrame({
        'id': np.array(id).astype(np.int),
        'id1': np.array(id1).astype(np.int),
        'id2': np.array(id2).astype(np.int),
        'int32_col': np.array(int32_col).astype(np.int),
    })
    print(df.head())
    df.to_csv("central_table.tbl", index=False, sep=',', header=False)

def gen_dim1():
    id = np.arange(num_dim1, dtype=np.int)
    fk = id + 10000
    int32_col = id
    int16_col = id
    df = pd.DataFrame({
        'id': np.array(id).astype(np.int),
        'fk': np.array(fk).astype(np.int),
        'int32_col': np.array(int32_col).astype(np.int),
        'int16_col': np.array(int16_col).astype(np.int),
    })
    print(df.head())
    df.to_csv("dimension_table1.tbl", index=False, sep=',', header=False)

def gen_dim2():
    id = np.arange(num_dim2)
    fk = id + 10000
    text_col = [str_values[i % len(str_values)] for i in range(num_dim2)]
    date_col = [date_values[i % len(str_values)] for i in range(num_dim2)]
    df = pd.DataFrame({
        'id': np.array(id).astype(np.int),
        'fk': np.array(fk).astype(np.int),
        'text_col': text_col,
        'date_col': date_col,
    })
    print(df.head())
    df.to_csv("dimension_table2.tbl", index=False, sep=',', header=False)

def gen_dim3():
    id = np.arange(num_dim3)
    central_id = np.arange(num_central)
    fk = [central_id[i % num_central] for i in range(num_dim3)]
    int32_col = np.arange(num_dim3, dtype=np.int)
    float32_col = [float_values[i % len(str_values)] for i in range(num_dim3)]
    float64_col = [float_values[i % len(str_values)] for i in range(num_dim3)]
    df = pd.DataFrame({
        'id': np.array(id).astype(np.int),
        'fk': np.array(fk).astype(np.int),
        'int32_col': np.array(int32_col).astype(np.int),
        'float32_col': float32_col,
        'float64_col': float64_col,
    })
    print(df.head())
    df.to_csv("dimension_table3.tbl", index=False, sep=',', header=False)


def gen_dim4():
    id = np.arange(num_dim4)
    central_id = np.arange(num_central)
    fk = [central_id[i%num_central] for i in range(num_dim4)]
    int8_col = [i % 256 for i in range(num_dim4)]
    char_col = [str_values[i % len(str_values)][0] for i in range(num_dim4)]
    df = pd.DataFrame({
        'id': np.array(id).astype(np.int),
        'fk': np.array(fk).astype(np.int),
        'int8_col': np.array(int8_col).astype(np.int),
        'char_col': char_col,
    })
    print(df.head())
    df.to_csv("dimension_table4.tbl", index=False, sep=',', header=False)


gen_central()
gen_dim1()
gen_dim2()
gen_dim3()
gen_dim4()