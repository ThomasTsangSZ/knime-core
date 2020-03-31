
package org.knime.core.data.store.arrow.table;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Schema;

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
public abstract class CachedVectorStore<T extends FieldVector> implements VectorStore<T> {

	// TODO: We probably want to replace this by a more powerful (= actual) cache
	// implementation.
	// TODO: We could also try to combine Arrow's manual reference counting with
	// a SoftReference cache. E.g., by wrapping a vector in an object that
	// releases the vector's buffers in its finalize method and putting such
	// wrappers in the cache.
	private final List<T> m_cache = new ArrayList<>();

	private final ReentrantReadWriteLock m_cacheLock = new ReentrantReadWriteLock(true);

	// i-th entry in cache is not necessarily i-th chunk of the overall table.
	// Entries in cache are contiguous, however, so maintaining a simple offset
	// should be sufficient for matching. TODO: Actually do that.
	private final int m_cacheOffset = 0;

	private final VectorToDiskWriter<T> m_writer;

	// TODO: Also multiple readers here? (Since we have multiple read accesses per
	// column. Then we would need to change the design of our store layer,
	// however.)
	private final VectorFromDiskReader<T> m_reader;

	public CachedVectorStore(final File baseDirectory, final long vectorIndex, final Schema vectorSchema,
		final BufferAllocator allocator) throws IOException
	{
		final File file = new File(baseDirectory, Long.toString(vectorIndex));
		file.createNewFile();
		m_writer = new VectorToDiskWriter<>(file, vectorSchema, allocator);
		m_reader = null; // TODO
	}

	/**
	 * @return A new, {@link FieldVector#allocateNew() pre-allocated} chunk.
	 */
	protected abstract T createNewChunk();

	@Override
	public T getNextVectorForWriting() {
		final T vector = createNewChunk();
		// Done on client's behalf.
		ArrowUtils.retainVector(vector);
		return vector;
	}

	@Override
	public void returnLastWritteOnVector(final T vector) {
		m_cacheLock.writeLock().lock();
		try {
			// Done on client's behalf.
			m_cache.add(vector);
			ArrowUtils.releaseVector(vector);
		}
		finally {
			m_cacheLock.writeLock().unlock();
		}
	}

	@Override
	public boolean hasVectorForReading(final long index) {
		throw new IllegalStateException("not yet implemented"); // TODO: implement
	}

	@Override
	public T getVectorForReading(long index) {
		m_cacheLock.readLock().lock();
		try {
			index -= m_cacheOffset;
			if (0 >= index || index < m_cache.size()) {
				final T vector = m_cache.get(Math.toIntExact(index));
				// Done on client's behalf. We do this here instead of in the client to
				// make retrieving the vector from cache and retaining its buffers
				// atomic.
				ArrowUtils.retainVector(vector);
				return vector;
			}
			else {
				// TODO: Upgrade lock to "write", read vector from disk, put into cache.
			}
		}
		finally {
			m_cacheLock.readLock().unlock();
		}
	}

	@Override
	public void returnReadFromVector(final long index, final T vector) {
		throw new IllegalStateException("not yet implemented"); // TODO: implement
		// ArrowUtils.releaseVector(vector);
	}

	public void flush() throws IOException {
		m_cacheLock.writeLock().lock();
		try {
			final List<T> toFlush = new ArrayList<>(m_cache);
			m_cache.clear();
			// TODO: Do this sync or async? Async would be faster but could cause
			// memory problems if flush was called due to a memory alert, since then
			// writing new data into the table is re-enabled (lock lifted) while still
			// spilling old data to disk.
			for (final T vector : toFlush) {
				m_writer.write(vector);
				vector.close();
			}
		}
		finally {
			m_cacheLock.writeLock().unlock();
		}
	}
}
