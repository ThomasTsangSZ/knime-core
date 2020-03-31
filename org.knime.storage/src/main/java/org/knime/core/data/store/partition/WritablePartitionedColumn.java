package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.value.WritableValueAccess;

public class WritablePartitionedColumn<T> implements WritableColumn {

	/*
	 * Accessors to store
	 */
	private final ColumnPartitionWritableValueAccess<T> m_valueAccess;

	private ColumnPartitionStore<T> m_columnStore;

	private ColumnPartition<T> m_currentPartition;

	/*
	 * Indices used by the implementation
	 */
	private int m_currentPartitionMaxIndex = -1;

	private long m_index = -1;

	private long m_currentPartitionIndex = 0;

	// TODO typing? store has to match access or line 43 will crash.
	public WritablePartitionedColumn(ColumnPartitionStore<T> store) {
		m_columnStore = store;
		m_valueAccess = store.createLinkedWriteAccess();

		switchToNextPartition();
	}

	@Override
	public void fwd() {
		if (++m_index < m_currentPartitionMaxIndex) {
			m_valueAccess.incIndex();
		} else {
			switchToNextPartition();
			m_index = 0;
		}
	}

	private void switchToNextPartition() {
		try {
			if (m_currentPartition != null)
				m_currentPartition.close();
			m_currentPartition = m_columnStore.getOrCreatePartition(m_currentPartitionIndex++);
			m_valueAccess.updatePartition(m_currentPartition);
			m_currentPartitionMaxIndex = m_currentPartition.getValueCapacity() - 1;

		} catch (Exception e) {
			// TODO Exception handling
			throw new RuntimeException(e);
		}
	}

	@Override
	public WritableValueAccess getValueAccess() {
		return m_valueAccess;
	}

	@Override
	public void close() throws Exception {
		if (m_currentPartition != null) {
			m_currentPartition.close();
		}
	}
}