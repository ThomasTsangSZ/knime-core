package org.knime.core.data.vector.table;

import org.knime.core.data.table.WritableTable;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.table.column.ReadableColumn;
import org.knime.core.data.table.column.ReadableColumnCursor;
import org.knime.core.data.table.column.WritableColumnCursor;
import org.knime.core.data.vector.ReadableVectorStore;
import org.knime.core.data.vector.VectorStoreRoot;

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

			final ReadableVectorStore<Long, ?> vectorGroup = store
					.createStore(schemas[i].getColumnType().getNativeTypes());
			m_writableColumnCursors[i] = new WritableColumnCursor() {
				@Override
				public ReadableColumnCursor createCursor() {
					// increase reference counter for each created cursor
					// TODO thread-safety retain (what is someone releases before)
					vectorGroup.retain();
					return new VectorReadableColumnCursor<>(vectorGroup);
				}
			};
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
