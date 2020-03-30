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
- next() rausziehen
- MultiVecValues (oder wie auch immer der shit dann heißt), die ein eigenes schema haben). Eigtl. ist das nix anderes als nen chunk :smile: 
- Domain Calculation
- DuplicateChecker + RowId.

### Impl Baustellen:
- Chunking
- Caching
- Disc IO
- Testing

### KNIME baustellen:
- Serialisierung von Stores
- FileStores & BlobStores
- CollectionCells
- ComplexCells which need to be serialisied (+special caching of these as we don't want to call byte[] serialize(cell) on each read/write).