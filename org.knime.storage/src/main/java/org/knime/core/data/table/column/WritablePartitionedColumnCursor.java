
package org.knime.core.data.table.column;

import org.knime.core.data.table.value.WritableValue;

public final class WritablePartitionedColumnCursor<T> implements WritableColumnCursor {

	private final PartitionValue<T> m_linkedAccess;

	private Partition<T> m_currentPartition;

	private long m_currentPartitionMaxIndex = -1;

	private long m_index = -1;

	private final WritablePartitionedColumn<T> m_store;

	public WritablePartitionedColumnCursor(final WritablePartitionedColumn<T> store) {
		m_linkedAccess = store.createLinkedValue();
		m_store = store;
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
			closeCurrentPartition(m_index);
			m_currentPartition = m_store.extend().getRight();
			m_linkedAccess.updatePartition(m_currentPartition);
			m_currentPartitionMaxIndex = m_currentPartition.getCapacity() - 1;
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void closeCurrentPartition(long numValues) throws Exception {
		if (m_currentPartition != null) {
			m_currentPartition.setNumValues((int) numValues);
			// can be closed. we're done writing
			m_currentPartition.close();
			m_currentPartition = null;
		}
	}

	@Override
	public WritableValue getValueAccess() {
		return m_linkedAccess;
	}

	@Override
	public void close() throws Exception {
		closeCurrentPartition(m_index + 1);
	}
}
