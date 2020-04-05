
package org.knime.core.data.partition;

import org.knime.core.data.table.column.ReadableColumnCursor;
import org.knime.core.data.table.value.ReadableValue;

public final class ReadablePartitionedColumnCursor<T> implements ReadableColumnCursor {

	private final PartitionValue<T> m_linkedAccess;

	private Partition<T> m_currentPartition;

	private long m_currentPartitionMaxIndex = -1;

	private long m_index = -1;

	private long m_partitionIndex = -1;

	private PartitionStore<T> m_store;

	public ReadablePartitionedColumnCursor(final PartitionStore<T> store) {
		m_linkedAccess = store.createLinkedValue();
		m_store = store;
		switchToNextPartition();
	}

	@Override
	public boolean canFwd() {
		return m_index < m_currentPartitionMaxIndex
				// TODO
				|| m_partitionIndex < m_store.getNumPartitions() - 1;
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
			m_currentPartition = m_store.get(m_partitionIndex);
			m_linkedAccess.updatePartition(m_currentPartition);
			m_currentPartitionMaxIndex = m_currentPartition.getNumValuesWritten() - 1;
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
