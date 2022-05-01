# Smart IDs
This is a standard `cmake` project, so you need cmake to build it. See `libs.sh` for more info on how to build for aws ec2 ubuntu 20.04 machines.

Most of the code is in `src/` directory. The names of the subdirectories are self-explanatory (`optimizer/` contains the optimizer code, `execution/` is the execution engine). `src/main.cpp` contains experiments.

Directories named `*_workload/` contain data and queries. In particular, `job_light_workload/` is used for most experiments.
