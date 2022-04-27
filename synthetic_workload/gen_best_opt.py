import pandas as pd
import numpy as np
import os

# Who knows; I don't!


# From 64MB to 4GB in increments of 64MB.
import sys

size_budgets = [(i + 1)*(1 << 26) for i in range(64)]
data_dir = "synthetic_workload_data"
default_file = f"{data_dir}/default_cost.csv"
opt_dir = f"{data_dir}/opts"
cols = ["opt_name", "extra_size", "query_name", "cost"]
default_df = pd.read_csv(default_file, names=cols, index_col=False)
print(default_df.head())


if (sys.argv[1] == "mat_view"):
    mat_view_file = f"{data_dir}/mat_cost.csv"
    cols = ["opt_name", "num_rows", "extra_size", "query_name", "cost"]
    mat_df = pd.read_csv(mat_view_file, names=cols, index_col=False)
    print(mat_df.head())


if (sys.argv[1] == "index"):
    index_join_file = f"{data_dir}/index_join_cost.csv"
    cols = ["opt_name", "extra_size", "query_name", "cost_savings"]
    idx_df = pd.read_csv(index_join_file, names=cols, index_col=False)
    idx_df = idx_df.groupby(by=['opt_name', 'extra_size']).agg({"cost_savings": "sum"})
    idx_df.columns = ["cost_savings"]
    idx_df = idx_df.reset_index()
    idx_df["cost_savings_over_size"] = idx_df.cost_savings / idx_df.extra_size
    idx_df = idx_df.sort_values(by='cost_savings_over_size', ascending=False).reset_index(drop=True)
    print(idx_df.head())

if (sys.argv[1] == "smartid"):
    table_outs_file = f"{data_dir}/table_outputs.csv"
    filter_sels_file = f"{data_dir}/filter_sels.csv"
    outs_df = pd.read_csv(table_outs_file, names=['query_name', 'table_name', 'table_out', 'base_size'], index_col=False)
    sels_df = pd.read_csv(filter_sels_file, names=['query_name', 'table_name', 'col_name', 'sel', 'base_size'], index_col=False)
    print(outs_df.head())
    print(sels_df.head())


def gen_best_indices(size_budget):
    to_select = [False for i in range(len(idx_df))]
    curr_extra_size = 0
    i = 0
    while curr_extra_size < size_budget and i < len(idx_df):
        if curr_extra_size + idx_df.extra_size[i] <= size_budget and idx_df.cost_savings[i] > 0:
            curr_extra_size += idx_df.extra_size[i]
            to_select[i] = True
        i += 1
    # print(idx_df[to_select].head())
    return idx_df[to_select][['opt_name']].reset_index(drop=True)


def parse_view_name(n):
    n = n[len("mat_view_"):]
    return set(n.split("___"))

def gen_best_mat_views(size_budget):
    best_current_cost = pd.DataFrame({
        "query_name": default_df.query_name,
        "best_current_cost": default_df.cost,
    })
    merged_df = pd.merge(mat_df, best_current_cost, left_on='query_name', right_on='query_name')
    curr_available_budget = size_budget
    curr_mat_df = merged_df

    result = []

    built_view_size = 0
    built_view_costs = dict()

    while len(curr_mat_df) > 0:
        # print("START OF LOOP")
        # print(curr_mat_df)
        # Update current best cost. In accordance with previouly built view.
        best_current_costs = []
        for i in range(len(curr_mat_df)):
            prev_best = curr_mat_df.best_current_cost[i]
            query_name = curr_mat_df.query_name[i]
            if query_name in built_view_costs:
                built_cost = built_view_costs[query_name]
                best_current_costs.append(min(prev_best, built_cost))
            else:
                best_current_costs.append(prev_best)
        curr_mat_df.best_current_cost = best_current_costs
        # print("MIDDLE OF LOOP After cost update")
        # print(curr_mat_df)
        # Update available size.
        curr_available_budget = curr_available_budget - built_view_size
        # Delete views that are not useful.
        to_keep = [True for i in range(len(curr_mat_df))]
        for i in range(len(curr_mat_df)):
            view_size = curr_mat_df.extra_size[i]
            cost = curr_mat_df.cost[i]
            best_current_cost = curr_mat_df.best_current_cost[i]
            if view_size > curr_available_budget:
                # print(f"Removing {i}")
                to_keep[i] = False
            if cost >= best_current_cost:
                # print(f"Removing {i}")
                to_keep[i] = False
        curr_mat_df = curr_mat_df[to_keep].reset_index(drop=True)
        curr_mat_df['cost_savings'] = (curr_mat_df.best_current_cost - curr_mat_df.cost)
        curr_mat_df['relative_cost_savings'] = curr_mat_df.cost_savings / curr_mat_df.best_current_cost
        # print("MIDDLE OF LOOP after useless removal")
        # print(curr_mat_df)
        if len(curr_mat_df) == 0: break
        tmp_df = curr_mat_df[curr_mat_df.extra_size <= curr_available_budget].groupby(by=['opt_name', 'extra_size']).agg({"cost_savings": "sum", "relative_cost_savings": "sum"})
        tmp_df.columns = ["cost_savings", "relative_cost_savings"]
        tmp_df = tmp_df.reset_index()
        # print("MIDDLE OF LOOP AGG")
        # print(tmp_df)

        tmp_df['cost_savings_over_size'] = tmp_df.cost_savings / tmp_df.extra_size
        tmp_df['relative_cost_savings_over_size'] = tmp_df.relative_cost_savings / tmp_df.extra_size
        tmp_df = tmp_df.sort_values(by='relative_cost_savings_over_size', ascending=False).reset_index(drop=True)[:1]
        built_view_name = tmp_df.opt_name[0]
        built_view_size = tmp_df.extra_size[0]
        built_view_df = curr_mat_df[curr_mat_df.opt_name == built_view_name].reset_index(drop=False)
        # print("BUILT VIEW 1")
        # print(built_view_df)

        built_view_costs = {built_view_df.query_name[i]: built_view_df.cost[i] for i in range(len(built_view_df))}
        # print("BUILT VIEW")
        # print(tmp_df)
        # print(f"Size={built_view_size}; costs={built_view_costs}")
        result.append(tmp_df[['opt_name']])

    return pd.concat(result)


def write_results(res, opt_name, budget):
    if not os.path.exists(opt_dir):
        os.makedirs(opt_dir)
    outfile = f"{opt_dir}/{opt_name}_{budget}.csv"
    print(f"Writing {outfile}")
    res.to_csv(outfile, sep=' ', header=False, index=False)


def gen_best_embeddings(num_free_bits=16, min_num_bits=2, total_id_size=64):
    queries = outs_df.query_name.unique()
    tables = outs_df.table_name.unique()
    query_names = []
    from_table_names = []
    from_col_names = []
    to_table_names = []
    to_out_sizes = []
    induced_sels = []
    effectivenesses = []
    for query_name in queries:
        query_outs = outs_df[outs_df.query_name == query_name].reset_index(drop=True)
        query_sels = sels_df[sels_df.query_name == query_name].reset_index(drop=True)
        for i in range(len(query_sels)):
            from_table_name = query_sels.table_name[i]
            from_col_name = query_sels.col_name[i]
            from_sel = query_sels.sel[i]
            from_base_size = query_sels.base_size[i]
            for j in range(len(query_outs)):
                to_table_name = query_outs.table_name[j]
                if from_table_name == to_table_name: continue
                to_out_size = query_outs.table_out[j]
                to_base_size = query_outs.base_size[j]
                induced_sel = from_sel
                if to_base_size < from_base_size:
                    induced_sel = min(1, induced_sel * (from_base_size / to_base_size))
                effectiveness = (1 - induced_sel) * to_out_size
                query_names.append(query_name)
                from_table_names.append(from_table_name)
                from_col_names.append(from_col_name)
                to_table_names.append(to_table_name)
                to_out_sizes.append(to_out_size)
                induced_sels.append(induced_sel)
                effectivenesses.append(effectiveness)
    effectiveness_df = pd.DataFrame({
        'query_name': query_names,
        'from_table': from_table_names,
        'from_col': from_col_names,
        'to_table': to_table_names,
        'to_out_size': to_out_sizes,
        'induced_sel': induced_sels,
        'effectiveness': effectivenesses,
    })
    print(effectiveness_df)
    # Get best embeddings for each table.
    res = []
    num_builds = 0
    while len(effectiveness_df) > 0 and num_builds < (3 if total_id_size==64 else 2):
        num_builds += 1
        tmp_df = effectiveness_df.groupby(by=['from_table', 'from_col', 'to_table']).agg({'effectiveness': 'sum'})
        tmp_df.columns = ['effectiveness']
        tmp_df = tmp_df.reset_index()
        tmp_df = tmp_df.sort_values(['to_table', 'effectiveness'], ascending=[True, False]).reset_index(drop=True)
        # if (curr_build == 0): print(tmp_df)
        # print(tmp_df)
        curr_res = []
        # print(effectiveness_df[effectiveness_df.query_name == 'query28'])
        for table_name in tables:
            table_embeddings = tmp_df[tmp_df.to_table == table_name].reset_index(drop=True)
            if len(table_embeddings) == 0: continue
            best_embeddings = table_embeddings[:1]
            from_table = best_embeddings.from_table[0]
            from_col = best_embeddings.from_col[0]
            to_table = best_embeddings.to_table[0]
            # Update effectiveness df
            affected_queries = effectiveness_df[(effectiveness_df.from_table == from_table) & (effectiveness_df.from_col == from_col) & (effectiveness_df.to_table == to_table)].reset_index(drop=True)
            affected_queries = {affected_queries.query_name[i]: affected_queries.effectiveness[i] for i in range(len(affected_queries))}
            new_effectivenesses = list(effectiveness_df.effectiveness)
            to_keep = [True for i in range(len(effectiveness_df))]
            for i in range(len(effectiveness_df)):
                query_name = effectiveness_df.query_name[i]
                if query_name not in affected_queries:
                    continue
                to_keep[i] = not((effectiveness_df.from_table[i] == from_table) and (effectiveness_df.from_col[i] == from_col) and (effectiveness_df.to_table[i] == to_table))
                if effectiveness_df.to_table[i] == to_table:
                    new_effectivenesses[i] -= affected_queries[query_name]
                    if new_effectivenesses[i] < 0: # Remove negative impact.
                        to_keep[i] = False
            effectiveness_df.effectiveness = new_effectivenesses
            effectiveness_df = effectiveness_df[to_keep].reset_index(drop=True)
            curr_res.append(best_embeddings)
        res.append(pd.concat(curr_res))
        # print(effectiveness_df[effectiveness_df.query_name == 'query28'])
    # First remove columns that would not be very useful.
    res_df = pd.concat(res).sort_values(['to_table', 'effectiveness'], ascending=[True, False]).reset_index(drop=True)
    if (num_free_bits < 16):
        res_df.effectiveness = np.sqrt(res_df.effectiveness)
    eff_sum = res_df.groupby(by='to_table').agg({'effectiveness': 'sum'})
    eff_sum.columns = ['effectiveness_sum']
    eff_sum = eff_sum.reset_index()
    res_eff_df = pd.merge(res_df, eff_sum, on='to_table')
    res_eff_df['effectiveness_ratio'] = res_eff_df.effectiveness / res_eff_df.effectiveness_sum
    print("First DF")
    print(res_eff_df)
    res_eff_df = res_eff_df[res_eff_df.effectiveness_ratio > (2/num_free_bits)]
    print("First DF AFTER ratio")
    print(res_eff_df)
    # Now allocate bits
    eff_sum = res_eff_df.groupby(by='to_table').agg({'effectiveness': 'sum'})
    eff_sum.columns = ['effectiveness_sum']
    eff_sum = eff_sum.reset_index()
    res_eff_df = pd.merge(res_eff_df[['from_table', 'from_col', 'to_table', 'effectiveness']], eff_sum, on='to_table')
    res_eff_df['effectiveness_ratio'] = res_eff_df.effectiveness / res_eff_df.effectiveness_sum
    res_eff_df['num_bits'] = (res_eff_df['effectiveness_ratio'] * num_free_bits).astype(np.int32)

    # Some tables won't have all bits allocated due to
    # floating point rounding down. Give remaining bits to top embeddings.
    total_num_bits = {}
    for i in range(len(res_eff_df)):
        to_table = res_eff_df.to_table[i]
        if to_table not in total_num_bits: total_num_bits[to_table] = 0
        total_num_bits[to_table] += res_eff_df.num_bits[i]
    new_num_bits = list(res_eff_df['num_bits'])
    bit_offsets = []
    curr_bit_offsets = {}
    for i in range(len(res_eff_df)):
        to_table = res_eff_df.to_table[i]
        if to_table not in curr_bit_offsets: curr_bit_offsets[to_table] = total_id_size - num_free_bits
        additional_bits = num_free_bits - total_num_bits[to_table]
        total_num_bits[to_table] += additional_bits
        new_num_bits[i] += additional_bits
        bit_offsets.append(curr_bit_offsets[to_table])
        curr_bit_offsets[to_table] += new_num_bits[i]
    res_eff_df.num_bits = new_num_bits
    res_eff_df["bit_offset"] = bit_offsets
    print("Final DF")
    print(res_eff_df)
    res = res_eff_df[['from_table', 'from_col', 'to_table', 'num_bits', 'bit_offset']]
    if not os.path.exists(opt_dir):
        os.makedirs(opt_dir)
    outfile = f"{opt_dir}/embeddings_opt_{num_free_bits}.csv"
    res.to_csv(outfile, sep=' ', header=False, index=False)
    pass


if sys.argv[1] == "index":
    for budget in [i*(1<<26) for i in (2, 4, 6, 8, 10)]:
        res = gen_best_indices(budget)
        write_results(res, "index", budget)
        write_results(res, "index", f"{budget / (1 << 20)}MB")

if sys.argv[1] == "mat_view":
    for budget in [i*(1<<26) for i in [2, 4, 6, 8, 10, 16, 32, 48, 64, 80, 96, 112, 128]]:
        res = gen_best_mat_views(budget)
        write_results(res, "mat_view", budget)
        write_results(res, "mat_view", f"{budget / (1 << 20)}MB")


if sys.argv[1] == "smartid":
    gen_best_embeddings(num_free_bits=8, min_num_bits=2, total_id_size=32)
    # gen_best_embeddings()