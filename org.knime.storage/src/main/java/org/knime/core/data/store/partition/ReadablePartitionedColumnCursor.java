package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.column.ReadableColumnCursor;
import org.knime.core.data.store.table.value.ReadableValueAccess;

public class ReadablePartitionedColumnCursor<T> //
		implements ReadableColumnCursor {

	/*
	 * Accesses to store
	 */
	private final ColumnPartitionReadableValueAccess<T> m_valueAccess;

	private ColumnPartitionStore<T> m_columnStore;

	private ColumnPartition<T> m_currentPartition;

	/*
	 * Indices used by implementation
	 */
	private long m_currentPartitionIndex = -1;

	private long m_currentBufferMaxIndex = -1;

	private long m_index = -1;

	public ReadablePartitionedColumnCursor(ColumnPartitionStore<T> store) {
		m_valueAccess = store.createLinkedReadAccess();
		m_columnStore = store;
		switchToNextBuffer();
	}

	@Override
	public boolean canFwd() {
		return m_index < m_currentBufferMaxIndex
				// TODO
				// NB: we have to call m_store.numPartitions over and over again as number of
				// partitions can change during reading. Idea: if we introduce a "wait" while
				// store is still open for writing, we may have streaming solved :-)
				|| m_currentPartitionIndex + 1 < m_columnStore.getNumPartitions();
	}

	@Override
	public void fwd() {
		if (++m_index < m_currentBufferMaxIndex) {
			m_valueAccess.incIndex();
		} else {
			m_index = 0;
			switchToNextBuffer();
		}
	}

	private void switchToNextBuffer() {
		try {
			if (m_currentPartition != null)
				m_currentPartition.close();

			m_currentPartition = m_columnStore.getOrCreatePartition(++m_currentPartitionIndex);
			m_valueAccess.updatePartition(m_currentPartition);
			m_currentBufferMaxIndex = m_currentPartition.getValueCount() - 1;
		} catch (Exception e) {
			// TODO handle exception
			throw new RuntimeException(e);
		}
	}

	@Override
	public ReadableValueAccess getValueAccess() {
		return m_valueAccess;
	}

	@Override
	public void close() throws Exception {
		m_currentPartition.close();
	}
}