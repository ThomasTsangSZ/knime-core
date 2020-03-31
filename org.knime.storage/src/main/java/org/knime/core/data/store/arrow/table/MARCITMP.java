package org.knime.core.data.store.arrow.table;
//
//package org.knime.core.data.store.buffer;
//
//import java.io.IOException;
//
//import org.knime.core.data.store.table.column.ColumnType;
//
///*
// * # Writing case:
// * - User creates table
// * - Starts writing to it
// * - Trigger from outside ("flush"): spill entire cache content to disk, in order!
// * - Release entire cache
// * - Repeat
// * - Observer threads: greedy async. writing of cache content (without removing from cache)
// *   to disk during all of the above, to be already done upon memory alert
// *   - See imglib2-cache: fetcher threads
// *   - Goal: preemptively write as much as possible
// * # Reading case:
// * - Entries are either already in memory or still on disk
// * - In memory: easy, just return cache entry
// * - On disk: load from disk, put into cache, return like in the in-memory case
// * - Optimization:
// * 	 - Pre-fetching next batches
// *   - Release batches if no iterator is open that may want to read batch (or use some other
// *     heuristic; but the former should be guaranteed)
// */
//public class DefaultPartitionedColumn implements SingleColumnPartitionStore {
//
////	// TODO: We probably want to replace this by a more powerful (= actual) cache
////	// implementation.
////	// TODO: We could also try to combine Arrow's manual reference counting with
////	// a SoftReference cache. E.g., by wrapping a vector in an object that
////	// releases the vector's buffers in its finalize method and putting such
////	// wrappers in the cache.
////	private final List<Buffer> m_cache = new ArrayList<>();
////
////	private final ReentrantReadWriteLock m_cacheLock = new ReentrantReadWriteLock(true);
////
////	// i-th entry in cache is not necessarily i-th chunk of the overall table.
////	// Entries in cache are contiguous, however, so maintaining a simple offset
////	// should be sufficient for matching. TODO: Actually do that.
////	private final int m_cacheOffset = 0;
//
//	private ColumnPartitionStore m_store;
//
//	private long m_columnIndex;
//
//	private int m_currentPartition;
//
////	private final VectorToDiskWriter<T> m_writer;
////
////	// TODO: Also multiple readers here? (Since we have multiple read accesses per
////	// column. Then we would need to change the design of our store layer,
////	// however.)
////	private final VectorFromDiskReader<T> m_reader;
//
//	public DefaultPartitionedColumn(final ColumnPartitionStore store, final long columnIndex) throws IOException {
////		final File file = new File(baseDirectory, Long.toString(vectorIndex));
////		file.createNewFile();
////		m_writer = new VectorToDiskWriter<>(file, vectorSchema, allocator);
////		m_reader = null; // TODO
//
//		m_columnIndex = columnIndex;
//		m_store = store;
//	}
//
//	@Override
//	public ColumnPartition getOrCreatePartition(long index) {
//		return m_store.getOrCreatePartition(ColumnType.DOUBLE, m_columnIndex, m_currentPartition++);
//	}
//
//	@Override
//	public long numPartitions() {
//		return m_store.numPartitionsForColumn(m_columnIndex);
//	}
//
////	@Override
////	public BufferAccess getOrCreate(long index) {
////		m_cacheLock.readLock().lock();
////		try {
////			index -= m_cacheOffset;
////			// TODO couldn't there be the case where buffers 0-10 are written to disc &&
////			// 10-20 are cached? (question for Marcel)
////			if (0 >= index || index < m_cache.size()) {
////				// Done on client's behalf. We do this here instead of in the client to
////				// make retrieving the vector from cache and retaining its buffers
////				// atomic.
////				// ArrowUtils.retainVector(buffer);
////
////				// TODO how can we "retain", i.e. make sure we don't destroy it? Check where it
////				// is actually destroyed in the life-cycle
////				return new DefaultBufferAccess(m_cache.get(Math.toIntExact(index)));
////			} else {
////				// TODO where do I get the column type from?
////				return new DefaultBufferAccess(m_store.getOrCreateBuffer(null, m_columnIndex, index));
////			}
////		} finally {
////			m_cacheLock.readLock().unlock();
////		}
////	}
//
////	public void flush() throws Exception {
////		m_cacheLock.writeLock().lock();
////		try {
////			final List<Buffer> toFlush = new ArrayList<>(m_cache);
////			m_cache.clear();
////			// TODO: Do this sync or async? Async would be faster but could cause
////			// memory problems if flush was called due to a memory alert, since then
////			// writing new data into the table is re-enabled (lock lifted) while still
////			// spilling old data to disk.
////
////			// TODO only write data which has not been written before. cache can comprise
////			// buffers which already have been persisted.
////			for (final Buffer buffer : toFlush) {
////				buffer.persist();
////				buffer.close();
////			}
////		} finally {
////			m_cacheLock.writeLock().unlock();
////		}
////	}
//
//}
