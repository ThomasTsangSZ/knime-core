package org.knime.core.data.vector.table;

import org.knime.core.data.table.WritableTable;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.table.column.WritableColumnCursor;
import org.knime.core.data.vector.VectorStoreRoot;

// TODO shared abstract class with VectorStoreReadbleTable
public class VectorStoreWritableTable implements WritableTable {

	private final ColumnSchema[] m_schema;
	private final WritableColumnCursor[] m_writableColumnCursors;
	private VectorStoreRoot<Long> m_store;

	public VectorStoreWritableTable(final ColumnSchema[] schemas, final VectorStoreRoot<Long> store) {
		// TODO do we have to retain() the store here? Problematic point: thread-safety
		m_schema = schemas;
		m_store = store;
		m_writableColumnCursors = new WritableColumnCursor[schemas.length];

		for (int i = 0; i < schemas.length; i++) {
			m_writableColumnCursors[i] = new VectorWritableColumnCursor<>(
					store.createWritableStore(schemas[i].getColumnType().getNativeTypes()));
		}
	}

	@Override
	public WritableColumnCursor getWritableColumnCursor(final long columnIndexLong) {
		final int columnIndex = Math.toIntExact(columnIndexLong);
		return m_writableColumnCursors[columnIndex];
	}

	@Override
	public long getNumColumns() {
		return m_schema.length;
	}

	@Override
	public void close() throws Exception {
		// release reference on store.
		m_store.release();
	}
}
