package org.knime.core.data.partition;

import org.knime.core.data.table.WritableTable;
import org.knime.core.data.table.column.WritableColumn;
import org.knime.core.data.table.column.WritableColumnCursor;

public class WritablePartitionedTable implements WritableTable {

	private Store m_root;

	public WritablePartitionedTable(Store root) {
		m_root = root;
	}

	@Override
	public long getNumColumns() {
		return m_root.getNumStores();
	}

	@Override
	public WritableColumn getWritableColumn(long columnIndex) {
		return new WritableColumn() {

			@Override
			public WritableColumnCursor createWritableCursor() {
				return new WritablePartitionedColumnCursor<>(m_root.getStoreAt(columnIndex));
			}
		};
	}
}
