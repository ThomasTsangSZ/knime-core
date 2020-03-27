    /**
     * DESIGN REQUIREMENTS (besides interface-driven, composition over inheritence, modularity)
     *  -> Enable (extensible) shared memory for python / java
     *  -> Allow API for proxy-types
     *  -> wide-table support
     *  -> backwards-compatibility
     *  -> ultra-lightweight processing. Any processing on KNIME side should be much faster than any TableIO.
     *  -> Predicate push-down and Filter-API
     *
     *  Nice to haves for later
     *  -> Chunking (for parallel read / write of data & distributed computing "KNIMETable layer with map/reduce like operations on-top")
     *  -> Expose columnar API to end-user
     */