package org.knime.core.data.store;

// TODO here we can implement caching INDEPENDENT from memory / storage layout.
public class CachedChunkStore<C extends Chunk> implements ChunkStore<C> {

	private ChunkStore<C> m_delegate;

	public CachedChunkStore(ChunkStore<C> delegate) {
		m_delegate = delegate;
	}

	@Override
	public void persist(C batch) {
		m_delegate.persist(batch);
	}

	@Override
	public C load(long idx) {
		return m_delegate.load(idx);
	}

	@Override
	public C createNext() {
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

}
