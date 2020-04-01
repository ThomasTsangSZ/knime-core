### DESIGN REQUIREMENTS (besides interface-driven, composition over inheritence, modularity)
- Enable (extensible) shared memory for python / java
- API for proxy-types
- wide-table support
- backwards-compatibility
- ultra-lightweight processing. Any processing on KNIME side should be much faster than any TableIO.
- Predicate push-down and Filter-API
 
#### Nice to haves for later
- Chunking (for parallel read / write of data & distributed computing "KNIMETable layer with map/reduce like operations on-top")
- Expose columnar API to end-user

### API baustellen:
- Arrow Package (encapsulate store and cache)
- Life-cycle management (close() vs. destroy() vs. ... finishWriting() 'can I read before I've serialized the entire table')
- Thread-safety (multi-read, cache,...). Locking per partition.
- Pre-fetching / pre-writing (async)

- Serialisierung - how do I restore state of a store or rather entire table (use-case: (i) knime has stored store or (ii) store created without prior writing).
- MultiVecValue & Custom data types (e.g. Date&Time, Text, Struct, PNG Images). Support for serializers. Forseeable problem: avoid constant serialization and deserialization into byte[]
- Domain Calculation

- DuplicateChecker for RowId
- Support for FileStores / BlobStores

- Off heap memory management
- ORC Backend / Parquet Backend?
- Test idea: with intermediate buffers


### KNIME baustellen:
- Serialisierung von Stores
- FileStores & BlobStores
- CollectionCells
- ComplexCells which need to be serialisied (+special caching of these as we don't want to call byte[] serialize(cell) on each read/write).