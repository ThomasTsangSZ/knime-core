
package org.knime.core.data.store.table.row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.knime.core.data.store.table.column.ReadableColumn;
import org.knime.core.data.store.table.column.impl.ReadableDoubleColumn;
import org.knime.core.data.store.table.row.impl.ReadableDoubleValue;

public final class ReadableRowIterator implements Iterator<Row<ReadableDataValue>>, AutoCloseable {

	private final ColumnBackedReadableRow m_row;

	public ReadableRowIterator(final List<ReadableColumn> columns) {
		m_row = new ColumnBackedReadableRow(columns);
	}

	@Override
	public boolean hasNext() {
		return m_row.canFwd();
	}

	@Override
	public Row<ReadableDataValue> next() {
		m_row.fwd();
		return m_row;
	}

	@Override
	public void close() throws Exception {
		m_row.close();
	}

	private static final class ColumnBackedReadableRow implements Row<ReadableDataValue>, AutoCloseable {

		private final List<ReadableColumn> m_columns;

		private final List<ReadableDataValue> m_dataValues;

		public ColumnBackedReadableRow(final List<ReadableColumn> columns) {
			m_columns = columns;
			m_dataValues = new ArrayList<>(columns.size());
			for (final ReadableColumn column : m_columns) {
				final ReadableDataValue dataValue;
				// TODO: Matching
				if (column instanceof ReadableDoubleColumn) {
					dataValue = new ReadableDoubleValue((ReadableDoubleColumn) column);
				}
				else {
					throw new IllegalStateException("not yet implemented");
				}
				m_dataValues.add(dataValue);
			}
		}

		private boolean canFwd() {
			return !m_columns.isEmpty() && m_columns.get(0).canFwd();
		}

		private void fwd() {
			for (final ReadableColumn column : m_columns) {
				column.fwd();
			}
		}

		@Override
		public long getNumValues() {
			return m_dataValues.size();
		}

		@Override
		public ReadableDataValue getValueAt(final int idx) {
			return m_dataValues.get(idx);
		}

		@Override
		public void close() throws Exception {
			for (final ReadableColumn column : m_columns) {
				column.close();
			}
		}
	}
}
