
package org.knime.core.data.store.chunk;

import org.knime.core.data.store.table.column.ColumnSchema;

// TODO here we can implement caching INDEPENDENT from memory / storage layout.
// TODO general idea: as long as we don't close a chunk, the chunk remains open in cache.
public class CachedChunkStore<V extends VecAccessible> implements ChunkStore<V> {

	private final ChunkStore<V> m_delegate;

	public CachedChunkStore(final ChunkStore<V> s) {
		m_delegate = s;
	}

	@Override
	public void persist(final V batch) {
		m_delegate.persist(batch);
	}

	@Override
	public V load(final long idx) {
		return m_delegate.load(idx);
	}

	@Override
	public V createNext() {
		return m_delegate.createNext();
	}

	@Override
	public void destroy() {
		m_delegate.destroy();
	}

	@Override
	public long numChunks() {
		return m_delegate.numChunks();
	}

	@Override
	public ColumnSchema schema() {
		return m_delegate.schema();
	}
}
