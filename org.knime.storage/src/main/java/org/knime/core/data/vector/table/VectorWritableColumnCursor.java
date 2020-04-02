
package org.knime.core.data.vector.table;

import org.knime.core.data.table.column.WritableColumnCursor;
import org.knime.core.data.table.value.WritableValue;
import org.knime.core.data.vector.Vector;
import org.knime.core.data.vector.VectorGroup;

public final class VectorWritableColumnCursor<T> implements WritableColumnCursor {

	private final VectorValue<T> m_linkedAccess;

	private Vector<T> m_currentPartition;

	private long m_currentPartitionMaxIndex = -1;

	private long m_index = -1;

	private long m_currentPartitionIndex = -1;

	private final VectorGroup<Long, T> m_store;

	public VectorWritableColumnCursor(final VectorGroup<Long, T> store) {
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
			closeCurrentPartition();
			m_currentPartition = m_store.getOrCreate(++m_currentPartitionIndex);
			m_linkedAccess.updatePartition(m_currentPartition);
			m_currentPartitionMaxIndex = m_currentPartition.getCapacity() - 1;
		} catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void closeCurrentPartition() throws Exception {
		if (m_currentPartition != null) {
			m_currentPartition.setNumValues((int) m_index);
			m_currentPartition.close();
		}
	}

	@Override
	public WritableValue getValueAccess() {
		return m_linkedAccess;
	}

	@Override
	public void close() throws Exception {
		closeCurrentPartition();
	}
}
