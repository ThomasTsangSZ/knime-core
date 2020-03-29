
package org.knime.core.data.store.table.column;

import java.util.List;

import org.knime.core.data.store.vec.VecSchema;

public final class DefaultWritableTable implements WritableTable {

	private final WritableColumn[] m_columns;

	public DefaultWritableTable(final List<VecSchema> schema) {
		m_columns = new WritableColumn[schema.size()];
		for (int i = 0; i < schema.size(); i++) {
			// TODO: Match schema (or probably rather KNIME's DataType or the like
			// since we're basically in Node-facing code) to corresponding writable
			// column implementations.
			// Allocate column/table store here?
		}
		throw new IllegalStateException("not yet implemented");
	}

	@Override
	public long getNumColumns() {
		return m_columns.length;
	}

	@Override
	public WritableColumn getColumnAt(final long index) {
		return m_columns[Math.toIntExact(index)];
	}

	@Override
	public void close() throws Exception {
		for (final WritableColumn column : m_columns) {
			column.close();
		}
	}
}
