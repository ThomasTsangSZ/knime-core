
package org.knime.core.data.cache;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.knime.core.data.table.column.Partition;

public final class SequentialCache<O> implements Flushable, AutoCloseable {

	private final List<Partition<O>> m_cache = new ArrayList<>();

	private final ReentrantReadWriteLock m_cacheLock = new ReentrantReadWriteLock(true);

	private final SequentialCacheFlusher<O> m_flusher;
	private final SequentialCacheLoader<O> m_loader;

	private long m_cacheOffset = 0;

	public SequentialCache(final SequentialCacheFlusher<O> flusher, final SequentialCacheLoader<O> loader) {
		m_flusher = flusher;
		m_loader = loader;
	}

	public void add(final Partition<O> entry) {
		m_cacheLock.writeLock().lock();
		try {
			m_cache.add(entry);
		} finally {
			m_cacheLock.writeLock().unlock();
		}
	}

	public Partition<O> get(final long index) throws IOException {
		m_cacheLock.readLock().lock();
		try {
			Partition<O> entry = m_cache.get(Math.toIntExact(index - m_cacheOffset));
			if (entry == null) {
				entry = m_loader.load(index);
				add(entry);
				// TODO thread-safety?
				// retain for external
				// TODO: Acquire write lock, update, etc.
			}
			return entry;
		} finally {
			m_cacheLock.readLock().unlock();
		}
	}

	@Override
	public void flush() throws IOException {
		m_cacheLock.writeLock().lock();
		try {
			for (final Partition<O> partition : m_cache) {
				m_flusher.flush(partition);
			}
			m_cacheOffset = m_cache.size();
			m_cache.clear();
		} finally {
			m_cacheLock.writeLock().unlock();
		}
	}

	public void clear() throws Exception {
		for (final Partition<O> obj : m_cache) {
			// TODO we may only close if no other external reference is kept on this partition for reading or writing.
			obj.close();
		}
	}

	@Override
	public void close() throws Exception {
		flush();
		clear();
	}
}
