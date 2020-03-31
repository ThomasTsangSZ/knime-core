
package org.knime.core.data.store.arrow.table.column;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.arrow.table.VectorStore;
import org.knime.core.data.store.arrow.table.value.AbstractArrowWritableValueAccess;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.value.WritableValueAccess;

public final class DefaultArrowWritableColumn<V extends ValueVector> implements WritableColumn {

	private final AbstractArrowWritableValueAccess<V> m_access;

	private final VectorStore<V> m_vectorStore;

	private int m_currentVectorMaxIndex = -1;

	private long m_index = -1;

	// TODO interface 'ArrowWritableValueAccess' for
	// 'AbstractArrowWritableValueAccess'
	public DefaultArrowWritableColumn(final AbstractArrowWritableValueAccess<V> access,
			final VectorStore<V> vectorStore) {
		m_access = access;
		m_vectorStore = vectorStore;
		switchToNextVector();
	}

	@Override
	public void fwd() {
		if (++m_index < m_currentVectorMaxIndex) {
			m_access.incIndex();
		} else {
			switchToNextVector();
			m_index = 0;
		}
	}

	private void switchToNextVector() {
		returnCurrentVector();
		final V nextVector = m_vectorStore.getNextVectorForWriting();
		m_access.setVector(nextVector);
		m_currentVectorMaxIndex = nextVector.getValueCapacity() - 1;
	}

	private void returnCurrentVector() {
		final V currentVector = m_access.getVector();
		if (currentVector != null) {
			m_vectorStore.returnLastWritteOnVector(currentVector);
		}
	}

	@Override
	public WritableValueAccess getValueAccess() {
		return m_access;
	}

	@Override
	public void close() throws Exception {
		returnCurrentVector();
	}
}
