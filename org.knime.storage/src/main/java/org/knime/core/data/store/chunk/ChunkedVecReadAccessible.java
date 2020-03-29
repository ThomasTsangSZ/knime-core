package org.knime.core.data.store.chunk;

import java.util.Iterator;

import org.knime.core.data.store.MutableValue;
import org.knime.core.data.store.Value;
import org.knime.core.data.store.vec.VecReadAccess;
import org.knime.core.data.store.vec.VecReadAccessible;
import org.knime.core.data.store.vec.VecAccessibleOnVecAccessibles;
import org.knime.core.data.store.vec.VecSchema;

public class ChunkedVecReadAccessible implements VecReadAccessible<Value> {

	private ChunkStore m_store;
	private VecAccessibleOnVecAccessibles<? extends Value> m_vecAccessible;

	public ChunkedVecReadAccessible(final ChunkStore store) {
		m_store = store;

		// TODO maybe don't use iterator here is we everywhere else use "access"
		// ... (brr.later!)
		m_vecAccessible = new VecAccessibleOnVecAccessibles<MutableValue>(store.schema(),
				new Iterable<VecReadAccessible<MutableValue>>() {
					@Override
					public Iterator<VecReadAccessible<MutableValue>> iterator() {
						return new Iterator<VecReadAccessible<MutableValue>>() {

							private long m_chunkIdx = 0;

							@Override
							public boolean hasNext() {
								return m_chunkIdx < m_store.numChunks();
							}

							@Override
							public VecReadAccessible<MutableValue> next() {
								return m_store.load(m_chunkIdx++);
							}
						};
					}
				});
	}

	@Override
	public VecSchema schema() {
		return m_store.schema();
	}

	@SuppressWarnings("unchecked")
	@Override
	public VecReadAccess<Value> access() {
		// TODO get rid of this cast.
		return (VecReadAccess<Value>) m_vecAccessible.access();
	}

}
