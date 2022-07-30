# Join-algorithms

This project implements two diff.erent join algorithms in order to evaluate their performances.
It uses the [Waterloo SPARQL Diversity Test Suite](https://dsg.uwaterloo.ca/watdiv/) to benchmark their performances.
Both algorithms use numpy arrays and allocate new memory when needed. The initial size of the output tables
is a factor of the worst case. Fiddeling with this `INITAL_OUTPUT_TABLE_SIZE_FACTOR` increase the performance, but one might run 
into RAM issues on the way.
If the numpy array is full, it will increase its size by `OUTPUT_TABLE_INCREMENT_SIZE`

## Hash Join

The implementation of hash join is using the standard pyton dictionary. It is allocating new memory when reaching the previously set limit.
The initial table size inside the dictionary can be ddjusted with `HASH_TABLE_INCREMENT_SIZE`.


## Sort Merge Join

The implementation of the sort merge join is using numpy nd.array. It is allocating memory when needed.
