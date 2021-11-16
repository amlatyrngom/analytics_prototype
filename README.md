# Smart IDs
Repo for Smart IDs.

## Current Features
* Table Storage.
* Loading tables (including TPCH tables) from csv.
* Simple Vectorized Query Engine:
  * Scan, Filter, Projection.
  * Hash Join:
    * Inner Join

## Next Features
* Query Engine Features:
    * Add Semi-Join (expected implementation + testing time: 2 hours).
    * Full Denormalization (expected implementation + testing time: 2 hours).
* Embeddings (unknown implementation time).
* Only if needed:
    * Indexes
    * NULL support.

## Testing
Make the working directory contains the `tpch_data` folder. This is needed for table loading. The `cmake` test targets are:
* `hash_join_test`: Test the hash join.
* `vector_ops_int_test`: Test integer vector integer. String & dates are handled in `scan_test`.
* `scan_test`: Test scans, filters and projections on all data types.
* `table_loader_test`: Test storage & loading from csv.