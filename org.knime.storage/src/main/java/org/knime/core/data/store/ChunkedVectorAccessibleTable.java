package org.knime.core.data.store;

import org.knime.core.data.store.vec.VecType;
import org.knime.core.data.store.vec.VecGroupAccessible;

public class ChunkedVectorAccessibleTable implements VecGroupAccessible {

	public <C extends Chunk> ChunkedVectorAccessibleTable(ChunkStore<C> store) {
		// TODO me like caching. huk!
		m_store = new CachedChunkStore<C>(store);
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub

	}

	@Override
	public VecType[] types() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

}
