### DESIGN REQUIREMENTS (besides interface-driven, composition over inheritence, modularity)
- Enable (extensible) shared memory for python / java
- API for proxy-types
- wide-table support
- backwards-compatibility
- ultra-lightweight processing. Any processing on KNIME side should be much faster than any TableIO.
- Predicate push-down and Filter-API

## TODOs

### API design of org.knime.data.store
- [X] Arrow Package (encapsulate store and cache)
- [ ] Test Life-cycle management (close() vs. destroy() vs. ... finishWriting() 'can I read before I've serialized the entire table')
- [ ] Test Thread-safety (multi-read, cache,...). Locking per partition.
- [X] MultiVecValue & Custom data types (e.g. Date&Time, Text, Struct, PNG Images). 
- [ ] Support for serializers. Forseeable problem: avoid constant serialization and deserialization into byte[]
- [ ] Try to access some data from python (Davin/Marcel -> shared memory, shared jni)
- [ ] Pre-fetching / pre-writing (async)
- [ ] Serialization - how do I restore state of a store or rather entire table (use-case: (i) knime has stored store or (ii) store created without prior writing).
- [ ] Exception Handling & Logging
- [ ] Domain Calculation
- [ ] DuplicateChecker for RowId
- [ ] Predicate push-down and Filter-API

### KNIME Integration
- [ ] CollectionCells
- [ ] Support for FileStores / BlobStores
- [ ] Off heap memory management

### Nice-to-haves
- [ ] ORC Backend / Parquet Backend?
- [ ] Test idea: with intermediate buffers
- [ ] Wide-table support (e.g. automatically wrap K-consecutive columns for doubles into a double[] behind the scenes.
- [ ] Use framework for streaming (NB: Nearly support read while write already today).
- [ ] user facing API improvements (see tests)
