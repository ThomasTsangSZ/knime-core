
package org.knime.core.data.store.arrow.table.column;

import java.util.function.Supplier;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.arrow.table.ArrowUtils;
import org.knime.core.data.store.arrow.table.VectorStore;
import org.knime.core.data.store.arrow.table.value.AbstractArrowReadableValueAccess;
import org.knime.core.data.store.table.column.ReadableColumn;
import org.knime.core.data.store.table.column.ReadableColumnIterator;
import org.knime.core.data.store.table.value.ReadableValueAccess;

public final class DefaultArrowReadableColumn<V extends ValueVector> implements ReadableColumn {

	private final Supplier<AbstractArrowReadableValueAccess<V>> m_accessFactory;

	private final VectorStore<V> m_vectorStore;

	public DefaultArrowReadableColumn(final Supplier<AbstractArrowReadableValueAccess<V>> accessFactory,
		final VectorStore<V> vectorStore)
	{
		m_accessFactory = accessFactory;
		m_vectorStore = vectorStore;
	}

	@Override
	public ReadableColumnIterator iterator() {
		return new DefaultArrowReadableColumnIterator<>(m_accessFactory.get(), m_vectorStore);
	}

	public static final class DefaultArrowReadableColumnIterator<V extends ValueVector> //
		implements ReadableColumnIterator
	{

		private final VectorStore<V> m_vectorStore;

		private final AbstractArrowReadableValueAccess<V> m_access;

		private long m_vectorIndex = -1;

		private int m_currentVectorMaxIndex = -1;

		public DefaultArrowReadableColumnIterator(final AbstractArrowReadableValueAccess<V> access,
			final VectorStore<V> vectorStore)
		{
			m_access = access;
			m_vectorStore = vectorStore;
			switchToNextVector();
		}

		@Override
		public boolean canFwd() {
			return m_access.getIndex() < m_currentVectorMaxIndex //
				|| m_vectorStore.hasVectorForReading(m_vectorIndex + 1);
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
			final V nextVector = m_vectorStore.getVectorForReading(++m_vectorIndex);
			ArrowUtils.retainVector(nextVector);
			m_access.setIndex(0);
			m_access.setVector(nextVector);
			m_currentVectorMaxIndex = nextVector.getValueCount() - 1;
		}

		private void releaseCurrentVector() {
			final V currentVector = m_access.getVector();
			if (currentVector != null) {
				ArrowUtils.releaseVector(currentVector);
			}
		}

		@Override
		public ReadableValueAccess getValueAccess() {
			return m_access;
		}

		@Override
		public void close() throws Exception {
			releaseCurrentVector();
		}
	}
}
