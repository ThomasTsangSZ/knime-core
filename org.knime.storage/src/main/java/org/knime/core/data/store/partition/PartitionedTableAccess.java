package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.column.ColumnType;
import org.knime.core.data.store.table.column.ReadableColumnCursor;
import org.knime.core.data.store.table.column.ReadableTable;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.column.WritableTable;
import org.knime.core.data.store.table.value.ReadableValueAccess;
import org.knime.core.data.store.table.value.WritableValueAccess;

// glue between store and table
public class PartitionedTableAccess implements ReadableTable, WritableTable {

	private ColumnPartitionStore[] m_columnPartitions;

	public PartitionedTableAccess(final TablePartitionStore store, ColumnType[] types) {
		for (int i = 0; i < types.length; i++) {
			m_columnPartitions[i] = store.add(types[i]);
		}
	}

	@Override
	public long getNumColumns() {
		return m_columnPartitions.length;
	}

	@Override
	public ReadableColumnCursor createReadableColumnCursor(long index) {
		return new ReadableBufferColumnCursor(m_columnPartitions[(int) index]);
	}

	@Override
	public WritableColumn getWritableColumn(long index) {
		// TODO do we need a singleton pattern for WColumns per index?
		return new WritableColumnPartitionStore(m_columnPartitions[(int) index]);
	}

	final class WritableColumnPartitionStore implements WritableColumn {

		private final ColumnPartitionWritableValueAccess m_valueAccess;

		private int m_currentPartitionMaxIndex = -1;

		private long m_index = -1;

		private long m_currentPartitionIndex = 0;

		private ColumnPartition m_currentPartition;

		private ColumnPartitionStore m_columnPartitionStore;

		public WritableColumnPartitionStore(ColumnPartitionStore store) {
			m_valueAccess = store.getWriteAccess();
			m_columnPartitionStore = store;

			switchToNextPartition();
		}

		@Override
		public void fwd() {
			if (++m_index < m_currentPartitionMaxIndex) {
				m_valueAccess.incIndex();
			} else {
				switchToNextPartition();
				m_index = 0;
			}
		}

		private void switchToNextPartition() {
			try {
				if (m_currentPartition != null)
					m_currentPartition.close();
				m_currentPartition = m_columnPartitionStore.getOrCreatePartition(m_currentPartitionIndex++);
				m_valueAccess.updateBufferAccess(m_currentPartition);
				m_currentPartitionMaxIndex = m_currentPartition.getValueCapacity() - 1;

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
			}
		}
	}

	final class ReadableBufferColumnCursor //
			implements ReadableColumnCursor {

		private final ColumnPartitionReadableValueAccess m_valueAccess;

		private long m_currentPartitionIndex = -1;

		private long m_currentBufferMaxIndex = -1;

		private long m_index = -1;

		private ColumnPartition m_currentBuffer;

		private ColumnPartitionStore m_columnStore;

		public ReadableBufferColumnCursor(ColumnPartitionStore store) {
			m_valueAccess = store.getReadAccess();
			m_columnStore = store;
			switchToNextBuffer();
		}

		@Override
		public boolean canFwd() {
			return m_index < m_currentBufferMaxIndex
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
				if (m_currentBuffer != null)
					m_currentBuffer.close();

				m_currentBuffer = m_columnStore.getOrCreatePartition(++m_currentPartitionIndex);
				m_valueAccess.updateBufferAccess(m_currentBuffer);
				m_currentBufferMaxIndex = m_currentBuffer.getValueCount() - 1;
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
			m_currentBuffer.close();
		}
	}

	@Override
	public void close() throws Exception {

	}
}
