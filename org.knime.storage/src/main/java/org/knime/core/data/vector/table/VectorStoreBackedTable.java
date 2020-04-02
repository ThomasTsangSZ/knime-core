
package org.knime.core.data.vector.table;

import org.knime.core.data.table.ReadableTable;
import org.knime.core.data.table.WritableTable;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.table.column.ReadableColumn;
import org.knime.core.data.table.column.ReadableColumnCursor;
import org.knime.core.data.table.column.WritableColumnCursor;
import org.knime.core.data.vector.VectorGroup;
import org.knime.core.data.vector.VectorStore;

public final class VectorStoreBackedTable implements ReadableTable, WritableTable {

	private final ColumnSchema[] m_schema;
	private final ReadableColumn[] m_readableColumns;
	private final WritableColumnCursor[] m_writableColumnCursors;
	private VectorStore<Long> m_store;

	public VectorStoreBackedTable(final ColumnSchema[] schemas, final VectorStore<Long> store) {
		m_schema = schemas;
		m_store = store;
		m_readableColumns = new ReadableColumn[schemas.length];
		m_writableColumnCursors = new WritableColumnCursor[schemas.length];
		// one store per column.
		for (int i = 0; i < schemas.length; i++) {

			final VectorGroup<Long, ?> vectorGroup = store
					.createGroup(schemas[i].getColumnType().getNativeTypes());
			m_readableColumns[i] = new ReadableColumn() {
				@Override
				public ReadableColumnCursor createCursor() {
					return new VectorReadableColumnCursor<>(vectorGroup);
				}
			};
			m_writableColumnCursors[i] = new VectorWritableColumnCursor<>(vectorGroup);
		}
	}

	@Override
	public long getNumColumns() {
		return m_schema.length;
	}

	@Override
	public ReadableColumn getReadableColumn(final long columnIndexLong) {
		final int columnIndex = Math.toIntExact(columnIndexLong);
		return m_readableColumns[columnIndex];
	}

	@Override
	public WritableColumnCursor getWritableColumnCursor(final long columnIndexLong) {
		final int columnIndex = Math.toIntExact(columnIndexLong);
		return m_writableColumnCursors[columnIndex];
	}

	@Override
	public void close() throws Exception {
		// release all memory!
		m_store.close();
	}

}
