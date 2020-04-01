package org.knime.core.data.store.column.partition;

import org.knime.core.data.store.column.WritableColumn;
import org.knime.core.data.store.column.value.WritableValueAccess;

public class PartitionedWritableColumn<T> implements WritableColumn {

	/*
	 * Accessors to store
	 */
	private final ColumnPartitionValueAccess<T> m_linkedAccess;

	private ColumnPartition<T> m_currentPartition;

	private ColumnPartitionFactory<T> m_factory;

	/*
	 * Indices used by the implementation
	 */
	private int m_currentPartitionMaxIndex = -1;

	private long m_index = -1;

	// TODO typing? store has to match access or line 43 will crash.
	public PartitionedWritableColumn(ColumnPartitionFactory<T> factory, ColumnPartitionValueAccess<T> linkedAccess) {
		m_factory = factory;
		m_linkedAccess = linkedAccess;

		switchToNextPartition();
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
			// closes current partition only...
			close();
			m_currentPartition = m_factory.createPartition();
			m_linkedAccess.updatePartition(m_currentPartition);
			m_currentPartitionMaxIndex = m_currentPartition.getCapacity() - 1;

		} catch (Exception e) {
			// TODO Exception handling
			throw new RuntimeException(e);
		}
	}

	@Override
	public WritableValueAccess getValueAccess() {
		return m_linkedAccess;
	}

	@Override
	public void close() throws Exception {
		if (m_currentPartition != null) {
			m_currentPartition.close();
			m_currentPartition.setNumValues((int) m_index);
		}
	}
}