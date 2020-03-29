
package org.knime.core.data.store.table.row;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.column.impl.WritableDoubleColumn;
import org.knime.core.data.store.table.row.impl.WritableDoubleValue;

// TODO: Other name. This is not an iterator. More similar to RowOutput or an access.
public final class WritableRowIterator implements AutoCloseable {

	private final ColumnBackedWritableRow m_row;

	public WritableRowIterator(final List<WritableColumn> columns) {
		m_row = new ColumnBackedWritableRow(columns);
	}

	public Row<WritableDataValue> next() {
		m_row.fwd();
		return m_row;
	}

	@Override
	public void close() throws Exception {
		m_row.close();
	}

	private static final class ColumnBackedWritableRow implements Row<WritableDataValue>, AutoCloseable {

		private final List<WritableColumn> m_columns;

		private final List<WritableDataValue> m_dataValues;

		public ColumnBackedWritableRow(final List<WritableColumn> columns) {
			m_columns = columns;
			m_dataValues = new ArrayList<>(columns.size());
			for (final WritableColumn column : m_columns) {
				final WritableDataValue dataValue;
				// TODO: Matching
				if (column instanceof WritableDoubleColumn) {
					dataValue = new WritableDoubleValue((WritableDoubleColumn) column);
				}
				else {
					throw new IllegalStateException("not yet implemented");
				}
				m_dataValues.add(dataValue);
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
		public WritableDataValue getValueAt(final int idx) {
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
