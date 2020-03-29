package org.knime.core.data.store.chunk;

import java.util.Iterator;

import org.knime.core.data.store.MutableValue;
import org.knime.core.data.store.vec.VecReadAccess;
import org.knime.core.data.store.vec.VecReadAccessible;
import org.knime.core.data.store.vec.VecAccessibleOnVecAccessibles;
import org.knime.core.data.store.vec.VecSchema;

public class ChunkedVecWriteAccessible implements VecReadAccessible<MutableValue> {

	private ChunkStore m_store;
	private VecAccessibleOnVecAccessibles<MutableValue> m_vecAccessible;

	public ChunkedVecWriteAccessible(final ChunkStore store) {
		m_store = store;
		m_vecAccessible = new VecAccessibleOnVecAccessibles<MutableValue>(store.schema(),
				new Iterable<VecReadAccessible<MutableValue>>() {
					@Override
					public Iterator<VecReadAccessible<MutableValue>> iterator() {
						return new Iterator<VecReadAccessible<MutableValue>>() {
							@Override
							public boolean hasNext() {
								return true;
							}

							@Override
							public VecReadAccessible<MutableValue> next() {
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
	public VecReadAccess<MutableValue> access() {
		return m_vecAccessible.access();
	}

}
