
package org.knime.core.data.vector.table;

import org.knime.core.data.table.ReadableTable;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.table.column.ReadableColumn;
import org.knime.core.data.table.column.ReadableColumnCursor;
import org.knime.core.data.vector.ReadableVectorStore;
import org.knime.core.data.vector.ReadableVectorStoreFactory;

public final class VectorStoreReadableTable implements ReadableTable {

	private final ColumnSchema[] m_schema;
	private final ReadableColumn[] m_readableColumns;

	public VectorStoreReadableTable(final ColumnSchema[] schemas, final ReadableVectorStoreFactory store) {
		m_schema = schemas;
		m_readableColumns = new ReadableColumn[schemas.length];

		for (int i = 0; i < schemas.length; i++) {
			final ReadableVectorStore<?> vectorGroup = store
					.createReadableStore(schemas[i].getColumnType().getNativeTypes());
			m_readableColumns[i] = new ReadableColumn() {
				@Override
				public ReadableColumnCursor createCursor() {
					// increase reference counter for each created cursor
					// TODO thread-safety retain (what is someone releases before)
					return new VectorReadableColumnCursor<>(vectorGroup);
				}
			};
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
}
