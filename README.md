# Target
Launch a mini kudu cluster: 1 master with 3 tservers with local kudu-master, kudu-tserver, and kudu. Provide some basic table statistic check.

# Usage
Specify the required values in env.sh.
## Start master
./cli.sh -m 1 -s
## Stop master
./cli.sh -m 1 -d
## Start tservers
./cli.sh -t 1 -s
./cli.sh -t 2 -s
./cli.sh -t 3 -s
## Stop tservers
./cli.sh -t 1 -d
./cli.sh -t 2 -d
./cli.sh -t 3 -d
## Check cluster status
./cli.sh -u
## List all tables
./cli.sh -l
