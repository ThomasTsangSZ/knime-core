
package org.knime.core.data.vector.cache;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class SequentialCache<O extends AutoCloseable> implements Flushable, AutoCloseable {

	private final List<O> m_cache = new ArrayList<>();

	private final ReentrantReadWriteLock m_cacheLock = new ReentrantReadWriteLock(true);

	private final SequentialCacheFlusher<O> m_flusher;

	private long m_cacheOffset = 0;

	public SequentialCache(final SequentialCacheFlusher<O> flusher) {
		m_flusher = flusher;
	}

	public void add(final O vector) {
		m_cacheLock.writeLock().lock();
		try {
			m_cache.add(vector);
		}
		finally {
			m_cacheLock.writeLock().unlock();
		}
	}

	public O get(final long index, final SequentialCacheLoader<O> loader) throws IOException {
		m_cacheLock.readLock().lock();
		try {
			O entry = m_cache.get(Math.toIntExact(index - m_cacheOffset));
			if (entry == null) {
				entry = loader.load(index);
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
			for (final O partition : m_cache) {
				m_flusher.flush(partition);
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
