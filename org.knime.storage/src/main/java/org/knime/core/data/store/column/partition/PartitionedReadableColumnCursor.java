package org.knime.core.data.store.column.partition;

import org.knime.core.data.store.column.ReadableColumnCursor;
import org.knime.core.data.store.column.value.ReadableValueAccess;

public class PartitionedReadableColumnCursor<T> //
		implements ReadableColumnCursor {

	/*
	 * Constants
	 */
	private final ColumnPartitionValueAccess<T> m_linkedAccess;

	private final ColumnPartitionReader<T> m_reader;

	/*
	 * Indices used by implementation
	 */
	private ColumnPartition<T> m_currentPartition;

	private long m_currentBufferMaxIndex = -1;

	private long m_index = -1;

	public PartitionedReadableColumnCursor(final ColumnPartitionReader<T> reader,
			final ColumnPartitionValueAccess<T> linkedValue) {
		m_linkedAccess = linkedValue;
		m_reader = reader;
		switchToNextBuffer();
	}

	@Override
	public boolean canFwd() {
		return m_index < m_currentBufferMaxIndex - 1
				// TODO
				|| m_reader.hasNext();
	}

	@Override
	public void fwd() {
		if (++m_index > m_currentBufferMaxIndex) {
			m_index = 0;
			switchToNextBuffer();
		}
		m_linkedAccess.incIndex();
	}

	private void switchToNextBuffer() {
		try {
			if (m_currentPartition != null)
				m_currentPartition.close();

			m_currentPartition = m_reader.next();
			m_linkedAccess.updatePartition(m_currentPartition);
			m_currentBufferMaxIndex = m_currentPartition.getNumValues() - 1;
		} catch (Exception e) {
			// TODO handle exception
			throw new RuntimeException(e);
		}
	}

	@Override
	public ReadableValueAccess getValueAccess() {
		return m_linkedAccess;
	}

	@Override
	public void close() throws Exception {
		if (m_currentPartition != null) {
			m_currentPartition.close();
		}
		m_reader.close();
	}
}