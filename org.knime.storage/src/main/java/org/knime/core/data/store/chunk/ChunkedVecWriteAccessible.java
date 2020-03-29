
package org.knime.core.data.store.chunk;

import java.util.Iterator;

import org.knime.core.data.store.vec.VecAccessible;
import org.knime.core.data.store.vec.VecAccessibleOnVecAccessibles;
import org.knime.core.data.store.vec.rw.VecReadAccess;
import org.knime.core.data.store.vec.rw.VecWriteAccess;

public class ChunkedVecWriteAccessible implements VecAccessible {

	private ChunkStore<?> m_store;
	private VecAccessibleOnVecAccessibles m_vecAccessible;

	public ChunkedVecWriteAccessible(final ChunkStore<?> store) {
		m_store = store;
		m_vecAccessible = new VecAccessibleOnVecAccessibles(() -> new Iterator<VecAccessible>() {

			@Override
			public boolean hasNext() {
				return true;
			}

			@Override
			public VecAccessible next() {
				return m_store.createNext();
			}
		});
	}

	@Override
	public VecWriteAccess getWriteAccess() {
		return m_vecAccessible.getWriteAccess();
	}

	@Override
	public VecReadAccess createReadAccess() {
		return m_vecAccessible.createReadAccess();
	}
}
