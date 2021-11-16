import numpy as np
import pandas as pd

num_rows = 1000000
mod_val = 10

# Keep these values to prevent slow nl join.
small_build_table_size = 2100  # Slightly more than one vector..
small_probe_table_size = 4200  # Slightly more than two vectors.

# These can be larger because they are only for hash joins.
large_build_table_size = 150000
large_probe_table_size = 1500000

# These are for testing embedding. Number should be powers of two over 1024.
# Make sure probe_size is double build_size so every build row has 2 matches.
# The intermediary table is redundant because the match is one-to-many. It is used for testing.
embedding_build_size = 2048
embedding_probe_size = 4096



# Constants used with table
str_values = ["aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii", "jjj"]
float_values = [0.37, 0.73, 3.7, 7.3, 0.037, 0.073, 0.0037, 0.0073, 0.00037, 0.00073]
date_values = ["1996-01-02", "1996-12-01", "1993-10-14", "1995-10-11", "1994-07-30", "1992-02-21", "1996-01-10",
               "1995-07-16", "1993-10-27", "1998-07-21"]


def gen_embedding_build():
    col1 = np.arange(0, embedding_build_size)  # PK
    df = pd.DataFrame({'col1': col1})
    df.to_csv("{}.tbl".format("embedding_build"), index=False, sep='|', header=False)


def gen_embedding_probe():
    col1 = np.arange(0, embedding_probe_size)  # PK
    col2 = [i % embedding_build_size for i in range(embedding_probe_size)]  # FK
    col3_firsthalf = [i for i in range(embedding_build_size)]
    col3_secondhalf = [embedding_build_size - i - 1 for i in range(embedding_build_size)]
    col3 = col3_firsthalf + col3_secondhalf  # Column to embed
    df = pd.DataFrame({'col1': col1, 'col2': col2, 'col3': col3})
    df.to_csv("{}.tbl".format("embedding_probe"), index=False, sep='|', header=False)


def gen_embedding_interm():
    col1 = np.arange(0, embedding_probe_size)  # Origin Key
    col2 = [i % embedding_build_size for i in range(embedding_probe_size)]  # Target Key
    df = pd.DataFrame({'col1': col1, 'col2': col2})
    df.to_csv("{}.tbl".format("embedding_interm"), index=False, sep='|', header=False)


def gen_build_table(build_table_name, build_size):
    col1 = np.arange(0, build_size)
    np.random.shuffle(col1)
    col2 = col1
    col3 = [str(val) for val in col1]
    df = pd.DataFrame({'col1': col1, 'col2': col2, 'col3': col3})
    print(df.head())
    df.to_csv("{}.tbl".format(build_table_name), index=False, sep='|', header=False)


def gen_probe_table(probe_table_name, build_size, probe_size):
    col1 = np.arange(0, probe_size)
    col2 = np.random.randint(0, build_size, probe_size)
    col3 = col2
    col4 = [str(val) for val in col2]
    df = pd.DataFrame({'col1': col1, 'col2': col2, 'col3': col3, 'col4': col4})
    print(df.head())
    df.to_csv("{}.tbl".format(probe_table_name), index=False, sep='|', header=False)


def gen_test1():
    col1 = [i for i in range(num_rows)]
    col2 = [i % mod_val for i in range(num_rows)]
    df = pd.DataFrame({'col1': col1, 'col2': col2})
    print(df.head())
    df.to_csv("test1.tbl", index=False, sep='|', header=False)


def gen_test2():
    col1 = [i for i in range(num_rows)]
    col2 = [i % mod_val for i in range(num_rows)]  # np.random.randint(0, 9, num_rows)
    col3 = [str_values[i] for i in col2]
    df = pd.DataFrame({'col1': col1, 'col2': col2, 'col3': col3})
    print(df.head())
    df.to_csv("test2.tbl", index=False, sep='|', header=False)


def gen_test3():
    col1 = [i for i in range(num_rows)]
    col2 = [i % mod_val for i in range(num_rows)]
    col3 = [str_values[i] for i in col2]
    col4 = [float_values[i] for i in col2]
    col5 = [float_values[i] + 1.0 for i in col2]
    col6 = [date_values[i] for i in col2]
    df = pd.DataFrame({'col1': col1, 'col2': col2, 'col3': col3, 'col4': col4, 'col5': col5, 'col6': col6})
    print(df.head())
    df.to_csv("test3.tbl", index=False, sep='|', header=False)


gen_embedding_build()
gen_embedding_probe()
gen_embedding_interm()
gen_build_table("small_build_table", small_build_table_size)
gen_build_table("large_build_table", large_build_table_size)
gen_probe_table("small_probe_table", small_build_table_size, small_probe_table_size)
gen_probe_table("large_probe_table", large_build_table_size, large_probe_table_size)
gen_test1()
gen_test2()
gen_test3()
