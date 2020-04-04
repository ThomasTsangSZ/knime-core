package org.knime.core.data.partition;

import org.knime.core.data.table.ReadableTable;
import org.knime.core.data.table.column.ReadableColumn;
import org.knime.core.data.table.column.ReadableColumnCursor;

// TODO common super class with 'StoreBackedWritable'
public class ReadablePartitionedTable implements ReadableTable {

	private ReadableColumn[] m_column;

	public ReadablePartitionedTable(Store store) {
		m_column = new ReadableColumn[(int) store.getNumStores()];
		for (int i = 0; i < m_column.length; i++) {
			m_column[i] = new DefaultReadablePartitionedColumn<>(store.getStoreAt(i));
		}
	}

	@Override
	public ReadableColumn getReadableColumn(long columnIndex) {
		return m_column[(int) columnIndex];
	}

	@Override
	public long getNumColumns() {
		return m_column.length;
	}

	class DefaultReadablePartitionedColumn<D> implements ReadableColumn {

		private final PartitionStore<D> m_store;

		public DefaultReadablePartitionedColumn(PartitionStore<D> store) {
			m_store = store;
		}

		@Override
		public ReadableColumnCursor createCursor() {
			return new ReadablePartitionedColumnCursor<D>(m_store);
		}
	}
}
