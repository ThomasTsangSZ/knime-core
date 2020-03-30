
package org.knime.core.data.store;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.table.column.WritableColumn;

public abstract class AbstractArrowWritableColumn<V extends ValueVector> implements WritableColumn {

	private final VectorStore<V> m_vectorStore;

	protected V m_vector;

	protected int m_idx = 0;

	public AbstractArrowWritableColumn(final VectorStore<V> vectorStore) {
		m_vectorStore = vectorStore;
		m_vector = m_vectorStore.createNextVectorForWriting();
		// TODO: retain here and release once we get a new vector?
	}

	@Override
	public void fwd() {
		m_idx++;
		if (m_idx == m_vector.getValueCapacity() - 1) {
			m_vector = m_vectorStore.createNextVectorForWriting();
		}
	}
}
