
package org.knime.core.data.store.chunk;

import java.util.Iterator;

import org.knime.core.data.store.VecAccessible;
import org.knime.core.data.store.VecAccessibleOnVecAccessibles;
import org.knime.core.data.store.table.value.ReadableVectorAccess;
import org.knime.core.data.store.table.value.WritableVectorAccess;

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
	public WritableVectorAccess getWriteAccess() {
		return m_vecAccessible.getWriteAccess();
	}

	@Override
	public ReadableVectorAccess createReadAccess() {
		return m_vecAccessible.createReadAccess();
	}
}
