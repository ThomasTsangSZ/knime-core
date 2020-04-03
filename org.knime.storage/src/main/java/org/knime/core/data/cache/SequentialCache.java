
package org.knime.core.data.cache;

import java.io.Flushable;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.knime.core.data.table.column.Partition;

// TODO thread-safety...
// TODO sequential pre-loading etc
// TODO there must be smarter sequential caches out there
// Important: partitions must be flushed in order.
public final class SequentialCache<O> implements Flushable, AutoCloseable {

	private final Map<Long, Partition<O>> m_cache = new TreeMap<>();

	private final ReentrantReadWriteLock m_cacheLock = new ReentrantReadWriteLock(true);

	private final SequentialCacheFlusher<O> m_flusher;
	private final SequentialCacheLoader<O> m_loader;

	public SequentialCache(final SequentialCacheFlusher<O> flusher, final SequentialCacheLoader<O> loader) {
		m_flusher = flusher;
		m_loader = loader;
	}

	public void add(final long idx, final Partition<O> entry) {
		m_cacheLock.writeLock().lock();
		try {
			m_cache.put(idx, entry);
		} finally {
			m_cacheLock.writeLock().unlock();
		}
	}

	public Partition<O> get(final long index) throws IOException {
		m_cacheLock.readLock().lock();
		try {
			Partition<O> entry = m_cache.get(index);
			if (entry == null) {
				entry = m_loader.load(index);
				m_cache.put(index, entry);
				// TODO thread-safety?
				// retain for external
				// TODO: Acquire write lock, update, etc.
			}
			return entry;
		} finally {
			m_cacheLock.readLock().unlock();
		}
	}

	// TODO Memory alert or similar: Block adding new stuff to cache.
	@Override
	public void flush() throws IOException {
		m_cacheLock.writeLock().lock();
		try {
			for (Entry<Long, Partition<O>> entry : m_cache.entrySet()) {
				m_flusher.flush(entry.getValue());
			}
			m_cache.clear();
		} finally {
			m_cacheLock.writeLock().unlock();
		}
	}

	public void clear() throws Exception {
		for (final Partition<O> obj : m_cache.values()) {
			// TODO we may only close if no other external reference is kept on this
			// partition for reading or writing.
			obj.close();
		}
	}

	@Override
	public void close() throws Exception {
		flush();
		clear();

		m_loader.close();
		m_flusher.close();
	}

	public void closeForWriting() throws Exception {
		m_flusher.close();
	}
}
