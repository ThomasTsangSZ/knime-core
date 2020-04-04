package org.knime.core.data.partition;

import org.knime.core.data.table.WritableTable;
import org.knime.core.data.table.column.WritableColumnCursor;

public class WritablePartitionedTable implements WritableTable {

	private WritablePartitionedColumnCursor<?>[] m_cursors;

	public WritablePartitionedTable(Store root) {
		m_cursors = new WritablePartitionedColumnCursor[(int) root.getNumStores()];
		for (long i = 0; i < root.getNumStores(); i++) {
			m_cursors[(int) i] = new WritablePartitionedColumnCursor<>(root.getStoreAt(i));
		}

	}

	@Override
	public long getNumColumns() {
		return m_cursors.length;
	}

	@Override
	public WritableColumnCursor getWritableColumnCursor(long columnIndex) {
		return m_cursors[(int) columnIndex];
	}
}
