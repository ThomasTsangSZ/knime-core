
package org.knime.core.data.store.arrow.table.column;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.arrow.table.ArrowUtils;
import org.knime.core.data.store.arrow.table.VectorStore;
import org.knime.core.data.store.arrow.table.value.AbstractArrowWritableValueAccess;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.value.WritableValueAccess;

public final class DefaultArrowWritableColumn<V extends ValueVector> implements WritableColumn {

	private final AbstractArrowWritableValueAccess<V> m_access;

	private final VectorStore<V> m_vectorStore;

	private int m_currentVectorMaxIndex = -1;

	public DefaultArrowWritableColumn(final AbstractArrowWritableValueAccess<V> access,
		final VectorStore<V> vectorStore)
	{
		m_access = access;
		m_vectorStore = vectorStore;
		switchToNextVector();
	}

	@Override
	public void fwd() {
		final int index = m_access.getIndex();
		if (index < m_currentVectorMaxIndex) {
			m_access.setIndex(index + 1);
		}
		else {
			switchToNextVector();
		}
	}

	private void switchToNextVector() {
		releaseCurrentVector();
		final V nextVector = m_vectorStore.getNextVectorForWriting();
		ArrowUtils.retainVector(nextVector);
		m_access.setIndex(0);
		m_access.setVector(nextVector);
		m_currentVectorMaxIndex = nextVector.getValueCapacity() - 1;
	}

	private void releaseCurrentVector() {
		final V currentVector = m_access.getVector();
		if (currentVector != null) {
			ArrowUtils.releaseVector(currentVector);
		}
	}

	@Override
	public WritableValueAccess getValueAccess() {
		return m_access;
	}

	@Override
	public void close() throws Exception {
		releaseCurrentVector();
	}
}
