# MilevaDB

MilevaDB is a Write-Optimized EinsteinDB majority-vote replicated-log server for High Throughput, Low Latency, Real Time PRAM environments. EinsteinDB employs a novel Relativistic Linearizable Causal Consistent Transaction guarantee protocol by building Strong Causal Consistent consensus layers on eventually consistent general-purpose data stores.

MilevaDB (is) currently tested with Cassandra where we target peak throughput within a factor of two of all 'eventual consistent' operations. 

MilevaDB and EinsteinDB introduce an asynchronous on-line schema change protocol via MVCC. EinsteinDB and MilevaDB support repeatable read isolation levels which guarantee that any data read cannot change, if the transaction reads the same data again, it will find the previously read data in place, unchanged, and available to read.

MilevaDB server parses and barters bartolinaSQL (SQL text to abstract syntax tree(AST)) requests. MilevaDB resolves, type checks (AST to annotated AST), and Simplifies(annotated AST to annotated AST) ETL.

