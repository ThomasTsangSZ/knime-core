
package org.knime.core.data.store.arrow.table;

import org.apache.arrow.vector.ValueVector;

/*
 * # Writing case:
 * - User creates table
 * - Starts writing to it
 * - Trigger from outside ("flush"): spill entire cache content to disk, in order!
 * - Release entire cache
 * - Repeat
 * - Observer threads: greedy async. writing of cache content to disk during all of the above,
 *   to be already done upon memory alert
 *   - See imglib2-cache: fetcher threads
 *   - Goal preemptively write as much as possible
 * # Reading case:
 * - Entries are either already in memory or still on disk
 * - In memory: easy, just return cache entry
 * - On disk: load from disk, put into cache, return like in the in-memory case
 * - Optimization:
 * 	 - Pre-fetching next batches
 *   - Release batches if no iterator is open that may want to read batch (or use some other
 *     heuristic; but the former should be guaranteed)
 */
public abstract class CachedVectorStore<T> implements VectorStore<T> {

	private int m_currentlyWrittenOnChunkIndex = -1;

	private T m_currentlyWrittenOnChunk;

	@Override
	public T getNextVectorForWriting() {
		if (m_currentlyWrittenOnChunk != null) {
			// TODO: Transfer to cache. Cache takes care of eventually releasing the
			// vector, spilling it to disk, etc.
		}
		m_currentlyWrittenOnChunkIndex++;
		m_currentlyWrittenOnChunk = createNewChunk();
		return m_currentlyWrittenOnChunk;
	}

	@Override
	public boolean hasVectorForReading(final long index) {
		throw new IllegalStateException("not yet implemented"); // TODO: implement
	}

	@Override
	public T getVectorForReading(final long index) {
		// TODO: Check only makes sense if the same store instance is used for
		// reading and writing. Otherwise (e.g., if store was deserialized) write
		// index may just be -1.
		if (index >= m_currentlyWrittenOnChunkIndex) {
			throw new IllegalArgumentException("No readable chunk at index: " + index);
		}
	}

	/**
	 * @return A new, {@link ValueVector#allocateNew() pre-allocated} chunk.
	 */
	protected abstract T createNewChunk();
}
