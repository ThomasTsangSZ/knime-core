
package org.knime.core.data.partition;

import org.knime.core.data.table.column.WritableColumnCursor;
import org.knime.core.data.table.value.WritableValue;

public final class WritablePartitionedColumnCursor<T> implements WritableColumnCursor {

	private final PartitionValue<T> m_linkedValue;

	private Partition<T> m_currentPartition;

	private long m_currentPartitionMaxIndex = -1;

	private long m_index = -1;

	private PartitionStore<T> m_store;

	public WritablePartitionedColumnCursor(final PartitionStore<T> store) {
		m_linkedValue = store.createLinkedValue();
		m_store = store;
		switchToNextPartition();
	}

	@Override
	public void fwd() {
		if (++m_index > m_currentPartitionMaxIndex) {
			switchToNextPartition();
			m_index = 0;
		}
		m_linkedValue.incIndex();
	}

	private void switchToNextPartition() {
		try {
			closeCurrentPartition(m_index);
			m_currentPartition = m_store.createPartition();
			m_linkedValue.updatePartition(m_currentPartition);
			m_currentPartitionMaxIndex = m_currentPartition.getCapacity() - 1;
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void closeCurrentPartition(long numValues) throws Exception {
		if (m_currentPartition != null) {
			m_currentPartition.setNumValues((int) numValues);
			// can be closed. we're done writing.
			m_currentPartition.close();
			m_currentPartition = null;
		}
	}

	@Override
	public WritableValue getValueAccess() {
		return m_linkedValue;
	}

	@Override
	public void close() throws Exception {
		closeCurrentPartition(m_index + 1);
	}
}
