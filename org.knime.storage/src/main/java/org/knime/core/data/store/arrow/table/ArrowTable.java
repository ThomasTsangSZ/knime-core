package org.knime.core.data.store.arrow.table;

import java.io.IOException;

import org.knime.core.data.store.partition.ReadablePartitionedColumnCursor;
import org.knime.core.data.store.partition.WritablePartitionedColumn;
import org.knime.core.data.store.table.column.ColumnSchema;
import org.knime.core.data.store.table.column.ReadableColumnCursor;
import org.knime.core.data.store.table.column.ReadableTable;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.column.WritableTable;

public class ArrowTable implements ReadableTable, WritableTable {
	// TODO long list
	private ColumnSchema[] m_schema;

	private Store m_store;

	public ArrowTable(Store store, ColumnSchema[] schema) throws IOException {
		m_schema = schema;
		m_store = store;
	}

	@Override
	public ReadableColumnCursor createReadableColumnCursor(long columnIndex) {
		// TODO
		return new ReadablePartitionedColumnCursor(m_store.getOrCreate(columnIndex),
				m_store.createReadAccess(m_schema[(int) columnIndex].getType()));
	}

	@Override
	public WritableColumn getWritableColumn(long columnIndex) {
		// TODO singleton
		return new WritablePartitionedColumn(m_store.getOrCreate(columnIndex),
				m_store.createWriteAccess(m_schema[(int) columnIndex].getType()));
	}

	@Override
	public long getNumColumns() {
		return m_schema.length;
	}

	@Override
	public void close() throws Exception {
		// TODO (Q: free all memory OR closeForWriting?)
	}

}
