
package org.knime.core.data.store.table.row;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.store.table.column.WritableColumn;

// TODO: Other name. This is not an iterator. More similar to RowOutput or an access.
// TODO: Implemented against KNIME classes ('DataValue', 'DataCell', ...)
public final class WritableRowIterator implements AutoCloseable {

	private final ColumnBackedWritableRow m_row;

	public WritableRowIterator(final List<WritableColumn> columns) {
		m_row = new ColumnBackedWritableRow(columns);
	}

	public Row<WritableValueAccess> next() {
		m_row.fwd();
		return m_row;
	}

	@Override
	public void close() throws Exception {
		m_row.close();
	}

	private static final class ColumnBackedWritableRow implements Row<WritableValueAccess>, AutoCloseable {

		private final List<WritableColumn> m_columns;

		private final List<WritableValueAccess> m_dataValues;

		public ColumnBackedWritableRow(final List<WritableColumn> columns) {
			m_columns = columns;
			m_dataValues = new ArrayList<>(columns.size());
			for (final WritableColumn column : m_columns) {
				m_dataValues.add(column.get());
			}
		}

		private void fwd() {
			for (final WritableColumn column : m_columns) {
				column.fwd();
			}
		}

		@Override
		public long getNumValues() {
			return m_dataValues.size();
		}

		@Override
		public WritableValueAccess getValueAt(final int idx) {
			return m_dataValues.get(idx);
		}

		@Override
		public void close() throws Exception {
			for (final WritableColumn column : m_columns) {
				column.close();
			}
		}
	}
}
