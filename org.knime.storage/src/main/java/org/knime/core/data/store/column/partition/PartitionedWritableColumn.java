package org.knime.core.data.store.column.partition;

import org.knime.core.data.store.column.WritableColumn;
import org.knime.core.data.store.column.value.WritableValueAccess;

public class PartitionedWritableColumn<T> implements WritableColumn {

	/*
	 * Accessors to store
	 */
	private final ColumnPartitionValueAccess<T> m_valueAccess;

	private final ColumnPartitionStore<T> m_columnStore;

	private ColumnPartition<T> m_currentPartition;

	/*
	 * Indices used by the implementation
	 */
	private int m_currentPartitionMaxIndex = -1;

	private long m_index = -1;

	// TODO typing? store has to match access or line 43 will crash.
	public PartitionedWritableColumn(ColumnPartitionStore<T> store) {
		m_columnStore = store;
		m_valueAccess = store.createAccess();

		switchToNextPartition();
	}

	@Override
	public void fwd() {
		if (++m_index > m_currentPartitionMaxIndex) {
			switchToNextPartition();
			m_index = 0;
		}
		m_valueAccess.incIndex();
	}

	private void switchToNextPartition() {
		try {
			// closes current partition only...
			close();
			m_currentPartition = m_columnStore.appendPartition();
			m_valueAccess.updatePartition(m_currentPartition);
			m_currentPartitionMaxIndex = m_currentPartition.getCapacity() - 1;

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
			m_currentPartition.setNumValues((int) m_index);
		}
	}
}