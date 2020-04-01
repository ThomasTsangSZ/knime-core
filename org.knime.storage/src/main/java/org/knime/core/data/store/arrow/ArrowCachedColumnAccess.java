package org.knime.core.data.store.arrow;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.data.store.column.partition.ColumnPartition;
import org.knime.core.data.store.column.partition.ColumnPartitionReader;
import org.knime.core.data.store.column.partition.ColumnPartitionValueAccess;

/*
 * # Writing case:
 * - User creates table
 * - Starts writing to it
 * - Trigger from outside ("flush"): spill entire cache content to disk, in order!
 * - Release entire cache
 * - Repeat
 * - Observer threads: greedy async. writing of cache content (without removing from cache)
 *   to disk during all of the above, to be already done upon memory alert
 *   - See imglib2-cache: fetcher threads
 *   - Goal: preemptively write as much as possible
 * # Reading case:
 * - Entries are either already in memory or still on disk
 * - In memory: easy, just return cache entry
 * - On disk: load from disk, put into cache, return like in the in-memory case
 * - Optimization:
 * 	 - Pre-fetching next batches
 *   - Release batches if no iterator is open that may want to read batch (or use some other
 *     heuristic; but the former should be guaranteed)
 */

/* 
 * really stupid first implementation of a cache
 * Always as single PartitionStore per column
 */
// TODO Make all of the crap here thread-safe :-)
// TODO all of this is arrow independent...

// TODO Cache is behaving wrong:
// - 1) writers are never closed. When can we close them?
class ArrowCachedColumnAccess<T> implements ArrowColumnAccess<T>, Flushable {

	// TODO: We probably want to replace this by a more powerful (= actual) cache
	// implementation.
	// TODO: We could also try to combine our cache reference counting with
	// a SoftReference cache. E.g., by wrapping a vector in an object that
	// releases the vector's buffers in its finalize method and putting such
	// wrappers in the cache.
	private final ConcurrentHashMap<Long, CachedColumnPartition> CACHE = new ConcurrentHashMap<>();

	private ArrowColumnAccess<T> m_delegate;

	private long m_lastWritten = -1;

	private int m_numPartitions;

	public ArrowCachedColumnAccess(final ArrowColumnAccess<T> delegate) {
		m_delegate = delegate;
	}

	@Override
	public void destroy() throws Exception {
		close();
		m_delegate.destroy();
	}

	@Override
	public long getNumPartitions() {
		return m_numPartitions;
	}

	/**
	 * @return the ColumnPartition or null in case the {@link ColumnPartitionStore}
	 *         has been closed.
	 */
	@Override
	public ColumnPartitionReader<T> create() {
		return new ColumnPartitionReader<T>() {

			private long m_idx = 0;

			private final ColumnPartitionReader<T> m_delegateIterator = m_delegate.create();

			@Override
			public boolean hasNext() {
				return m_idx < m_delegate.getNumPartitions();
			}

			@Override
			public ColumnPartition<T> next() {
				// TODO is there a better way to lock?
				synchronized (CACHE) {
					CachedColumnPartition partition = CACHE.get(m_idx);
					if (partition == null) {
						partition = add(m_idx, m_delegateIterator.next());
						// loading from disc. not yet in cache.
					} else {
						m_delegateIterator.skip();
						partition.retain();
					}
					m_idx++;

					// do this only if it's a new partition
					return partition;
				}
			}

			@Override
			public void skip() {
				m_delegateIterator.skip();
				m_idx++;
			}

			@Override
			public void close() throws Exception {
				m_delegateIterator.close();
			}
		};
	}

	private CachedColumnPartition add(long partitionIndex, final ColumnPartition<T> partition) {
		if (!(partition instanceof ArrowCachedColumnAccess.CachedColumnPartition)) {
			final CachedColumnPartition cached = new CachedColumnPartition(partition);
			// Make sure cache is blocking closing
			cached.retain();
			CACHE.put(partitionIndex, cached);
			return cached;
		} else {
			// we're already tracking.
			return (ArrowCachedColumnAccess<T>.CachedColumnPartition) partition;
		}
	}

	/**
	 * Writes a node. In case {@link ColumnPartitionStore} has been closed prior to
	 * this call, nothing happens.
	 */
	@Override
	public synchronized void write(ColumnPartition<T> partition) throws IOException {
		// TODO: Do this sync or async? Async would be faster but could cause
		// memory problems if flush was called due to a memory alert, since then
		// writing new data into the table is re-enabled (lock lifted) while still
		// spilling old data to disk.
		// we don't need this guy anymore. removed from cache etc.
		m_delegate.write(partition);
	}

	/**
	 * Writes a node. In case {@link ColumnPartitionStore} has been closed prior to
	 * this call, nothing happens.
	 */
	@Override
	public void flush() throws IOException {
		// blocking while flushing!
		// BETTER SYNCHRONIZATION?
		try {
			synchronized (CACHE) {
				// TODO this will work even if we already have written e.g. partitions 0-10.
				// However at the cost of "isWritten" check for 0-10 :-( (later)
				for (long i = m_lastWritten + 1; i < CACHE.size(); i++) {
					// Sequential write.
					write(CACHE.get(i));
					removeFromCacheAndClose(i);
					m_lastWritten = i;
				}
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public synchronized ColumnPartition<T> appendPartition() {
		// immediately add a ref
		return add(m_numPartitions++, m_delegate.appendPartition());
	}

	@Override
	public ColumnPartitionValueAccess<T> createLinkedType() {
		return m_delegate.createLinkedType();
	}

	/**
	 * NB: It's not the responsibility of the columnar store to make sure it's
	 * flushed before close.
	 * 
	 * All memory will be released.
	 * 
	 */
	@Override
	public void close() throws Exception {
		// TODO always go through all?
		for (long i = 0; i < m_numPartitions; i++) {
			removeFromCacheAndClose(i);
		}
	}

	private void removeFromCacheAndClose(long partitionIndex) throws Exception {
		final CachedColumnPartition partition = CACHE.get(partitionIndex);
		if (partition != null) {
			synchronized (partition) {
				// release from cache
				partition.release();
				CACHE.remove(partitionIndex);
			}
		}
	}

	// TODO check thread-safety
	private class CachedColumnPartition implements ColumnPartition<T> {
		private final ColumnPartition<T> m_partitionDelegate;
		private int m_numValues;
		private AtomicInteger m_lock;

		public CachedColumnPartition(ColumnPartition<T> delegate) {
			m_partitionDelegate = delegate;

			// whoever requested it now retained the partition
			m_lock = new AtomicInteger(1);
		}

		@Override
		public int getCapacity() {
			return m_partitionDelegate.getCapacity();
		}

		// only close in-memory representation, however, keep disc if buffer was
		// written.
		@Override
		public void close() throws Exception {
			// Only close if all references are actually closed!
			synchronized (m_lock) {
				if (m_lock.decrementAndGet() == 0) {
					m_partitionDelegate.close();
				}
			}
		}

		@Override
		public T get() {
			// TODO do we need sync here?
			return m_partitionDelegate.get();
		}

		@Override
		public int getNumValues() {
			return m_numValues;
		}

		@Override
		public void setNumValues(int numValues) {
			m_numValues = numValues;
		}

		public void retain() {
			m_lock.incrementAndGet();
		}

		public void release() throws Exception {
			m_lock.decrementAndGet();

			// try close
			// TODO thread-safety
			close();
		}

	}
}
