package org.knime.core.data.store.partition;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.data.store.table.column.ColumnType;

/* 
 * really stupid first implementation of a cache
 * Always as single PartitionStore per column
 */
// TODO Make all of the crap here thread-safe :-)
public class CachedColumnPartitionStore implements ColumnPartitionStore, Flushable {

	// TODO: We probably want to replace this by a more powerful (= actual) cache
	// implementation.
	// TODO: We could also try to combine Arrow's manual reference counting with
	// a SoftReference cache. E.g., by wrapping a vector in an object that
	// releases the vector's buffers in its finalize method and putting such
	// wrappers in the cache.
	private final ConcurrentHashMap<Long, ColumnPartition> CACHE = new ConcurrentHashMap<>();

	private ColumnPartitionStore m_delegate;

	// TODO do I need to synchronize that?
	private AtomicInteger[] m_referenceCounter;
	private AtomicBoolean[] m_isWritten;

	public CachedColumnPartitionStore(final ColumnPartitionStore delegate) {
		m_delegate = delegate;
		m_referenceCounter = new AtomicInteger[(int) delegate.getNumPartitions()];
		m_isWritten = new AtomicBoolean[(int) delegate.getNumPartitions()];
		for (int i = 0; i < delegate.getNumPartitions(); i++) {
			m_referenceCounter[i] = new AtomicInteger(0);
			m_isWritten[i] = new AtomicBoolean(false);
		}
	}

	@Override
	public long getNumPartitions() {
		return m_delegate.getNumPartitions();
	}

	@Override
	public ColumnPartition getOrCreatePartition(long partitionIndex) {
		// TODO is there a better way to lock?
		final AtomicInteger lock = m_referenceCounter[(int) partitionIndex];
		synchronized (lock) {
			final ColumnPartition partition = CACHE.getOrDefault(partitionIndex,
					m_delegate.getOrCreatePartition(partitionIndex));
			// do this only if it's a new partition
			if (!(partition instanceof CachedColumnPartition)) {
				lock.getAndIncrement();
				return new CachedColumnPartition(partition, partitionIndex);
			} else {
				// we're already tracking.
				return partition;
			}
		}
	}

	@Override
	public void persist(long partitionIndex) throws IOException {
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

	// write to disc and release memory.
	@Override
	public void flush() throws IOException {
		// blocking while flushing!
		for (long i = 0; i < m_referenceCounter.length; i++) {
			// ... if someone is already persisting: thanks bye
			persist(i);

			// in case someone else is currently incrementing (...)
			final AtomicInteger lock = m_referenceCounter[(int) i];
			synchronized (lock) {
				if (lock.decrementAndGet() == 0) {
					try {
						CACHE.get(i).close();
					} catch (Exception e) {
						// TODO handle!!!
						throw new RuntimeException(e);
					}
				}
				CACHE.remove(i);
			}
		}
	}

	private class CachedColumnPartition implements ColumnPartition {
		private final ColumnPartition m_partitionDelegate;
		private final int m_partitionIndex;

		public CachedColumnPartition(ColumnPartition delegate, long partitionIdx) {
			m_partitionDelegate = delegate;
			m_partitionIndex = (int) partitionIdx;
			m_referenceCounter[(int) m_partitionIndex].incrementAndGet();
		}

		@Override
		public ColumnType getType() {
			return m_partitionDelegate.getType();
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
	}
}
