
package org.knime.core.data.store.column.partition.therealstore;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class SequentialPartitionCache<P extends AutoCloseable> implements Flushable, AutoCloseable {

	private final List<P> m_cache = new ArrayList<>();

	private final ReentrantReadWriteLock m_cacheLock = new ReentrantReadWriteLock(true);

	private final PartitionCacheFlusher<P> m_flusher;

	private long m_cacheOffset = 0;

	public SequentialPartitionCache(final PartitionCacheFlusher<P> flusher) {
		m_flusher = flusher;
	}

	public void addPartition(final P vector) {
		m_cacheLock.writeLock().lock();
		try {
			m_cache.add(vector);
		}
		finally {
			m_cacheLock.writeLock().unlock();
		}
	}

	public P getPartition(final long index, final PartitionCacheLoader<P> loader) throws IOException {
		m_cacheLock.readLock().lock();
		try {
			P entry = m_cache.get(Math.toIntExact(index - m_cacheOffset));
			if (entry == null) {
				entry = loader.loadPartition(index);
				// TODO: Acquire write lock, update, etc.
			}
			return entry;
		}
		finally {
			m_cacheLock.readLock().unlock();
		}
	}

	@Override
	public void flush() throws IOException {
		m_cacheLock.writeLock().lock();
		try {
			for (final P partition : m_cache) {
				m_flusher.flushPartition(partition);
			}
			m_cacheOffset = m_cache.size();
			m_cache.clear();
		}
		finally {
			m_cacheLock.writeLock().unlock();
		}
	}

	@Override
	public void close() throws Exception {
		flush();
	}
}
