# MilevaDB

MilevaDB is a Write-Optimized EinsteinDB majority-vote replicated-log server for High Throughput, Low Latency, Real Time PRAM environments. EinsteinDB employs a novel Relativistic Linearizable Causal Consistent Transaction guarantee protocol by building Strong Causal Consistent consensus layers on eventually consistent general-purpose data stores.

MilevaDB (is) currently tested with Cassandra where we target peak throughput within a factor of two of all 'eventual consistent' operations. 

MilevaDB and EinsteinDB introduce an asynchronous on-line schema change protocol via MVCC. EinsteinDB and MilevaDB support repeatable read isolation levels which guarantee that any data read cannot change, if the transaction reads the same data again, it will find the previously read data in place, unchanged, and available to read.

MilevaDB implements snapshot isolation consistency addressing the semantics of "read" and "write" operations on replicated data. At its core, The Soliton Cluster: EinsteinDB, MilevaDB, and CausetNet(coming soon) resolves the issue of concurrent writes to the same logical address from two writers at separate physical locations using local replicas obeying the "monotonic reads" model.

MilevaDB server parses and barters bartolinaSQL (SQL text to abstract syntax tree(AST)) requests. MilevaDB resolves, type checks (AST to annotated AST), and Simplifies(annotated AST to annotated AST) ETL workloads. MilevaDB works as the SQL computing layer and produces a replicated-log manifest which is propagated acrosss the clusters.

MilevaDB employs a declarative table partitioning scheme we've named 'Soliton Cluster' which aims to be combined with data wrappers bringing all around improvements to partitioned functionality. 

