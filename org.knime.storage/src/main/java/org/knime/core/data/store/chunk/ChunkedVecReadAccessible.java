
package org.knime.core.data.store.chunk;

import java.util.Iterator;

import org.knime.core.data.store.vec.VecAccessible;
import org.knime.core.data.store.vec.VecAccessibleOnVecAccessibles;
import org.knime.core.data.store.vec.rw.ReadableVectorAccess;
import org.knime.core.data.store.vec.rw.WritableVectorAccess;

public class ChunkedVecReadAccessible implements VecAccessible {

	private ChunkStore<?> m_store;
	private VecAccessibleOnVecAccessibles m_vecAccessible;

	public ChunkedVecReadAccessible(final ChunkStore<?> store) {
		m_store = store;

		// TODO maybe don't use iterator here is we everywhere else use "access"
		// ... (brr.later!)
		m_vecAccessible = new VecAccessibleOnVecAccessibles(() -> new Iterator<VecAccessible>() {

			private final long m_chunkIdx = 0;

			@Override
			public boolean hasNext() {
				return m_chunkIdx < m_store.numChunks();
			}

			// Reusable vec-accessible!!!!
			@Override
			public VecAccessible next() {
				// TODO load chunk into my updateableVectorAccessible
//						new VecAccessible() {
//
//							@Override
//							public VecSchema schema() {
//								return m_store.schema();
//							}
//
//							@Override
//							public VecAccess access() {
//								m_store.load(m_chunkIdx++);
//								return ;
//							}
//						};

//						return proxy;
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
