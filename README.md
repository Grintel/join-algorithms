# Join-algorithms

This project implements two different join algorithms in order to evaluate their performances.
It uses the [WWaterloo SPARQL Diversity Test Suite](https://dsg.uwaterloo.ca/watdiv/) to benchmark their performances.
Both algorithms use numpy arrays and allocate new memory when needed. The initial size of the output tables
is a factor of the worst case. Fiddeling with this `INITAL_OUTPUT_TABLE_SIZE_FACTOR` increase the performance, but one might run 
into RAM issues on the way.

## Hash Join

The implementation of hash join is using the standard pyton hash. It is allocation new memory when reaching the previously set limit.
