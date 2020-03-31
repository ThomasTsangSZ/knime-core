package org.knime.core.data.store.partition;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
	private AtomicInteger[] m_referenceCounter;
	private AtomicBoolean[] m_isWritten;

	private AtomicBoolean m_isClosed;

	public CachedColumnPartitionStore(final ColumnPartitionStore<T> delegate) {
		m_delegate = delegate;
		m_referenceCounter = new AtomicInteger[(int) delegate.getNumPartitions()];
		m_isWritten = new AtomicBoolean[(int) delegate.getNumPartitions()];
		for (int i = 0; i < delegate.getNumPartitions(); i++) {
			m_referenceCounter[i] = new AtomicInteger(0);
			m_isWritten[i] = new AtomicBoolean(false);
		}

		m_isClosed = new AtomicBoolean(false);
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
	public ColumnPartition<T> getOrCreatePartition(long partitionIndex) {
		if (!m_isClosed.get()) {
			// TODO is there a better way to lock?
			final AtomicInteger lock = m_referenceCounter[(int) partitionIndex];
			synchronized (lock) {
				final ColumnPartition<T> partition = CACHE.getOrDefault(partitionIndex,
						m_delegate.getOrCreatePartition(partitionIndex));
				// do this only if it's a new partition
				if (!(partition instanceof CachedColumnPartitionStore.CachedColumnPartition)) {
					lock.getAndIncrement();
					return new CachedColumnPartition(partition, partitionIndex);
				} else {
					// we're already tracking.
					return partition;
				}
			}
		}

		return null;
	}

	/**
	 * Writes a node. In case {@link ColumnPartitionStore} has been closed prior to
	 * this call, nothing happens.
	 */
	@Override
	public void persist(long partitionIndex) throws IOException {
		if (!m_isClosed.get()) {
			// TODO should we implement a re-try in case something goes wrong?

			// TODO: Do this sync or async? Async would be faster but could cause
			// memory problems if flush was called due to a memory alert, since then
			// writing new data into the table is re-enabled (lock lifted) while still
			// spilling old data to disk.
			// we don't need this guy anymore. removed from cache etc.
			if (!m_isWritten[((int) partitionIndex)].getAndSet(true)) {
				m_delegate.persist(partitionIndex);
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
			for (long i = 0; i < m_referenceCounter.length; i++) {
				// ... if someone is already persisting: thanks bye
				persist(i);
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
			m_referenceCounter[(int) m_partitionIndex].incrementAndGet();
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
			final AtomicInteger lock = m_referenceCounter[m_partitionIndex];
			synchronized (lock) {
				if (lock.decrementAndGet() == 0) {
					assert (m_isWritten[m_partitionIndex].get());
					m_delegate.close();
				}
			}
		}

		@Override
		public T get() {
			// TODO do we need sync here?
			return m_partitionDelegate.get();
		}
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
			for (long i = 0; i < m_referenceCounter.length; i++) {
				removeFromCacheAndClose(i);
			}
		}
	}

	private void removeFromCacheAndClose(long partitionIndex) {
		final AtomicInteger lock = m_referenceCounter[(int) partitionIndex];
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

	@Override
	public ColumnPartitionReadableValueAccess<T> createLinkedReadAccess() {
		return m_delegate.createLinkedReadAccess();
	}

	@Override
	public ColumnPartitionWritableValueAccess<T> createLinkedWriteAccess() {
		return m_delegate.createLinkedWriteAccess();
	}
}
