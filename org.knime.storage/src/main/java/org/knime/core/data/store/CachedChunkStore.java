package org.knime.core.data.store;

import org.knime.core.data.store.chunk.Chunk;
import org.knime.core.data.store.chunk.ChunkStore;
import org.knime.core.data.store.vec.VecSchema;

// TODO here we can implement caching INDEPENDENT from memory / storage layout.
// TODO general idea: as long as we don't close a chunk, the chunk remains open in cache.
public class CachedChunkStore implements ChunkStore {

	private ChunkStore m_delegate;

	public CachedChunkStore(ChunkStore s) {
		m_delegate = s;
	}

	@Override
	public void persist(Chunk batch) {
		m_delegate.persist(batch);
	}

	@Override
	public Chunk load(long idx) {
		return m_delegate.load(idx);
	}

	@Override
	public Chunk createNext() {
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
	public VecSchema schema() {
		return m_delegate.schema();
	}
}
