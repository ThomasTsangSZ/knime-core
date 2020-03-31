package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.column.ReadableColumnCursor;
import org.knime.core.data.store.table.value.ReadableValueAccess;

public class ReadablePartitionedColumnCursor<T> //
		implements ReadableColumnCursor {

	/*
	 * Accesses to store
	 */
	private final ColumnPartitionValueAccess<T> m_valueAccess;

	private ColumnPartition<T> m_currentPartition;

	/*
	 * Indices used by implementation
	 */
	private long m_currentBufferMaxIndex = -1;

	private long m_index = -1;

	private ColumnPartitionIterator<T> m_columnStoreIterator;

	public ReadablePartitionedColumnCursor(ColumnPartitionStore<T> store) {
		m_valueAccess = store.createAccess();
		m_columnStoreIterator = store.iterator();
		switchToNextBuffer();
	}

	@Override
	public boolean canFwd() {
		return m_index < m_currentBufferMaxIndex - 1
				// TODO
				|| m_columnStoreIterator.hasNext();
	}

	@Override
	public void fwd() {
		if (++m_index > m_currentBufferMaxIndex) {
			m_index = 0;
			switchToNextBuffer();
		}
		m_valueAccess.incIndex();
	}

	private void switchToNextBuffer() {
		try {
			if (m_currentPartition != null)
				m_currentPartition.close();

			m_currentPartition = m_columnStoreIterator.next();
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
		if (m_currentPartition != null)
			m_currentPartition.close();
	}
}