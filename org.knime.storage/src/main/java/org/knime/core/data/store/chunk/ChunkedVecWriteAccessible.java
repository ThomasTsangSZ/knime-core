package org.knime.core.data.store.chunk;

import java.util.Iterator;

import org.knime.core.data.store.vec.VecAccess;
import org.knime.core.data.store.vec.VecAccessible;
import org.knime.core.data.store.vec.VecAccessibleOnVecAccessibles;
import org.knime.core.data.store.vec.VecSchema;

public class ChunkedVecWriteAccessible implements VecAccessible {

	private ChunkStore m_store;
	private VecAccessibleOnVecAccessibles m_vecAccessible;

	public ChunkedVecWriteAccessible(final ChunkStore store) {
		m_store = store;
		m_vecAccessible = new VecAccessibleOnVecAccessibles(store.schema(), new Iterable<VecAccessible>() {
			@Override
			public Iterator<VecAccessible> iterator() {
				return new Iterator<VecAccessible>() {
					@Override
					public boolean hasNext() {
						return true;
					}

					@Override
					public VecAccessible next() {
						return m_store.createNext();
					}
				};
			}
		});
	}

	@Override
	public VecSchema schema() {
		return m_store.schema();
	}

	@Override
	public VecAccess access() {
		return m_vecAccessible.access();
	}

}
