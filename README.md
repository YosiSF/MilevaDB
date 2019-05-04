# MilevaDB

MilevaDB is a Write-Optimized EinsteinDB majority-vote replicated-log server. EinsteinDB employs a novel Relativistic Linearizable Causal Consistent Transaction guarantee protocol by building Strong Causal Consistent consensus layers on eventually consistent; Sequential general-purpose data stores. 

MilevaDB (is) currently tested with Cassandra where we target peak throughput within a factor of two of all 'eventual consistent' operations. 

MilevaDB and EinsteinDB introduce an asynchronous schema change protocl via MVCC. EinsteinDB and MilevaDB support repeatable read isolation level which guarantees that any data read cannot change, if the transaction reads the same data again, it will find the previously read data in place, unchanged, and available to read