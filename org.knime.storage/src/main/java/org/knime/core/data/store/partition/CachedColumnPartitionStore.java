package org.knime.core.data.store.partition;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
public class CachedColumnPartitionStore<T> implements ColumnPartitionStore<T>, Flushable {

	// TODO: We probably want to replace this by a more powerful (= actual) cache
	// implementation.
	// TODO: We could also try to combine our cache reference counting with
	// a SoftReference cache. E.g., by wrapping a vector in an object that
	// releases the vector's buffers in its finalize method and putting such
	// wrappers in the cache.
	private final ConcurrentHashMap<Long, ColumnPartition<T>> CACHE = new ConcurrentHashMap<>();

	private ColumnPartitionStore<T> m_delegate;

	// TODO do I need to synchronize that?
	private final List<AtomicInteger> m_referenceCounter = new ArrayList<>();
	private final List<AtomicBoolean> m_isWritten = new ArrayList<>();

	private AtomicBoolean m_isClosed;

	public CachedColumnPartitionStore(final ColumnPartitionStore<T> delegate) {
		m_delegate = delegate;
		m_isClosed = new AtomicBoolean(false);

		for (int i = 0; i < getNumPartitions(); i++) {
			m_referenceCounter.add(new AtomicInteger());
			m_isWritten.add(new AtomicBoolean());
		}
	}

	@Override
	public long getNumPartitions() {
		return m_delegate.getNumPartitions();
	}

	/**
	 * @return the ColumnPartition or null in case the {@link ColumnPartitionStore}
	 *         has been closed.
	 */
	@Override
	public ColumnPartitionIterator<T> iterator() {

		return new ColumnPartitionIterator<T>() {

			private long m_idx = 0;

			private final ColumnPartitionIterator<T> m_delegateIterator = m_delegate.iterator();

			@Override
			public boolean hasNext() {
				return m_delegateIterator.hasNext();
			}

			@Override
			public ColumnPartition<T> next() {
				if (!m_isClosed.get()) {
					// TODO is there a better way to lock?
					final AtomicInteger lock = m_referenceCounter.get((int) m_idx);
					synchronized (lock) {
						ColumnPartition<T> partition = CACHE.get(m_idx);
						if (partition == null) {
							partition = addToCache(m_idx, m_delegateIterator.next());
							// loading from disc. not yet in cache.
						} else {
							m_delegateIterator.skip();
						}
						lock.incrementAndGet();
						m_idx++;

						// do this only if it's a new partition
						return partition;
					}
				}
				return null;
			}

			@Override
			public void skip() {
				m_delegateIterator.skip();
			}
		};
	}

	private ColumnPartition<T> addToCache(long partitionIndex, final ColumnPartition<T> partition) {
		if (!(partition instanceof CachedColumnPartitionStore.CachedColumnPartition)) {
			final CachedColumnPartitionStore<T>.CachedColumnPartition cached = new CachedColumnPartition(partition,
					partitionIndex);
			CACHE.put(partitionIndex, cached);
			return cached;
		} else {
			// we're already tracking.
			return partition;
		}
	}

	/**
	 * Writes a node. In case {@link ColumnPartitionStore} has been closed prior to
	 * this call, nothing happens.
	 */
	@Override
	public void persist(ColumnPartition<T> partition) throws IOException {
		if (!m_isClosed.get()) {
			// TODO should we implement a re-try in case something goes wrong?

			// TODO: Do this sync or async? Async would be faster but could cause
			// memory problems if flush was called due to a memory alert, since then
			// writing new data into the table is re-enabled (lock lifted) while still
			// spilling old data to disk.
			// we don't need this guy anymore. removed from cache etc.
			final int idx = (int) partition.getPartitionIndex();
			if (!m_isWritten.get(idx).getAndSet(true)) {
				m_delegate.persist(partition);
			}
		}
	}

	/**
	 * Writes a node. In case {@link ColumnPartitionStore} has been closed prior to
	 * this call, nothing happens.
	 */
	@Override
	public void flush() throws IOException {
		if (!m_isClosed.get()) {
			// blocking while flushing!
			for (long i = 0; i < m_referenceCounter.size(); i++) {
				// ... if someone is already persisting: thanks bye
				persist(CACHE.get(i));
				removeFromCacheAndClose(i);
			}
		}
	}

	@Override
	public synchronized ColumnPartition<T> appendPartition() {
		m_isWritten.add(new AtomicBoolean(false));
		// immediately add a ref
		m_referenceCounter.add(new AtomicInteger(1));
		return addToCache(m_isWritten.size() - 1, m_delegate.appendPartition());
	}

	@Override
	public ColumnPartitionValueAccess<T> createAccess() {
		return m_delegate.createAccess();
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
		if (!m_isClosed.getAndSet(true)) {
			for (long i = 0; i < m_referenceCounter.size(); i++) {
				removeFromCacheAndClose(i);
			}
		}
	}

	private class CachedColumnPartition implements ColumnPartition<T> {
		private final ColumnPartition<T> m_partitionDelegate;
		private final int m_partitionIndex;

		public CachedColumnPartition(ColumnPartition<T> delegate, long partitionIdx) {
			m_partitionDelegate = delegate;
			m_partitionIndex = (int) partitionIdx;
			m_referenceCounter.get((int) m_partitionIndex).incrementAndGet();
		}

		@Override
		public int getValueCount() {
			return m_partitionDelegate.getValueCount();
		}

		@Override
		public int getValueCapacity() {
			return m_partitionDelegate.getValueCapacity();
		}

		// only close in-memory representation, however, keep disc if buffer was
		// written.
		@Override
		public void close() throws Exception {
			// Only close if all references are actually closed!
			final AtomicInteger lock = m_referenceCounter.get(m_partitionIndex);
			synchronized (lock) {

				// TODO we need the '<0' because it could be called from outside
				// (#removeFromCacheAndClose). We should fix that
				// by adding lock / written indicators into this object
				if (lock.decrementAndGet() < 0) {
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
		public long getPartitionIndex() {
			return m_partitionDelegate.getPartitionIndex();
		}
	}

	private void removeFromCacheAndClose(long partitionIndex) {
		final AtomicInteger lock = m_referenceCounter.get((int) partitionIndex);
		synchronized (lock) {
			// decrement cache reference
			if (lock.decrementAndGet() == 0) {
				try {
					CACHE.get(partitionIndex).close();
				} catch (Exception e) {
					// TODO handle!!!
					throw new RuntimeException(e);
				}
			}
			CACHE.remove(partitionIndex);
		}
	}

}
