package org.knime.core.data.store.table;

import org.knime.core.data.store.CachedChunkStore;
import org.knime.core.data.store.Chunk;
import org.knime.core.data.store.ChunkStore;

public class DummyTable implements ChunkedTable {

	private ChunkStore<?> m_store;

	private Chunk m_current;

	private Chunk m_currChunk;

	private long m_chunkIdx;

	/**
	 * TODO FIX DESIGN FAILURE: Ideally we would only create read accesses once and
	 * then fill them with the actual new chunk. at the moment we would have to
	 * recreate all readers for each chunk. 
	 * 
	 * TODO also, here I'm only interested in READ Access
	 *
	 */

	public <C extends Chunk> DummyTable(ChunkStore<C> store) {
		// TODO me like caching. huk!
		m_store = new CachedChunkStore<C>(store);
	}

	@Override
	public boolean hasNext() {
		return m_store.numChunks() < m_readChunks && m_current.
	}

	@Override
	public Row next() {
		if (m_currChunk == null) {
			m_store.load(m_chunkIdx);
			m_chunkIdx++;
		}
		return m_currChunk.get;
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub

	}

}
