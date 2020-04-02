
package org.knime.core.data.vector.table;

import org.knime.core.data.table.column.ReadableColumnCursor;
import org.knime.core.data.table.value.ReadableValue;
import org.knime.core.data.vector.ReadableVectorStore;
import org.knime.core.data.vector.Vector;

public final class VectorReadableColumnCursor<T> implements ReadableColumnCursor {

	private final VectorValue<T> m_linkedAccess;

	private final ReadableVectorStore<T> m_vectorStore;

	private Vector<T> m_currentPartition;

	private long m_currentPartitionMaxIndex = -1;

	private long m_index = -1;

	private long m_partitionIndex = -1;

	public VectorReadableColumnCursor(final ReadableVectorStore<T> vectorGroup) {
		m_linkedAccess = vectorGroup.createLinkedValue();
		m_vectorStore = vectorGroup;
		switchToNextPartition();
	}

	@Override
	public boolean canFwd() {
		return m_index < m_currentPartitionMaxIndex
				// TODO
				|| m_partitionIndex < m_vectorStore.numVectors();
	}

	@Override
	public void fwd() {
		if (++m_index > m_currentPartitionMaxIndex) {
			switchToNextPartition();
			m_index = 0;
		}
		m_linkedAccess.incIndex();
	}

	private void switchToNextPartition() {
		try {
			m_partitionIndex++;
			closeCurrentPartition();
			m_currentPartition = m_vectorStore.get(m_partitionIndex);
			m_linkedAccess.updatePartition(m_currentPartition);
			m_currentPartitionMaxIndex = m_currentPartition.getNumValues() - 1;
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void closeCurrentPartition() throws Exception {
		if (m_currentPartition != null) {
			m_currentPartition.close();
			m_currentPartition = null;
		}
	}

	@Override
	public ReadableValue getValueAccess() {
		return m_linkedAccess;
	}

	@Override
	public void close() throws Exception {
		closeCurrentPartition();
	}
}
