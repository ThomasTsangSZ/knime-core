
package org.knime.core.data.store.column.partition;

import org.knime.core.data.store.column.ReadableColumnCursor;
import org.knime.core.data.store.column.value.ReadableValueAccess;

public final class PartitionedReadableColumnCursor<T> implements ReadableColumnCursor {

	private final PartitionedReadableValueAccess<T> m_linkedAccess;

	private final ColumnPartitionReader<T> m_reader;

	private ColumnPartition<T> m_currentPartition;

	private long m_currentPartitionMaxIndex = -1;

	private long m_index = -1;

	public PartitionedReadableColumnCursor(final PartitionedReadableValueAccess<T> linkedAccess,
		final ColumnPartitionReader<T> reader)
	{
		m_linkedAccess = linkedAccess;
		m_reader = reader;
		switchToNextPartition();
	}

	@Override
	public boolean canFwd() {
		return m_index < m_currentPartitionMaxIndex
		// TODO
			|| m_reader.hasNext();
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
			m_currentPartition = m_reader.next();
			m_linkedAccess.updatePartition(m_currentPartition);
			m_currentPartitionMaxIndex = m_currentPartition.getNumValues() - 1;
		}
		catch (final Exception e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	private void closeCurrentPartition() throws Exception {
		if (m_currentPartition != null) {
			m_currentPartition.close();
		}
		m_reader.close();
	}

	@Override
	public ReadableValueAccess getValueAccess() {
		return m_linkedAccess;
	}

	@Override
	public void close() throws Exception {
		closeCurrentPartition();
	}
}
