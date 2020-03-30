
package org.knime.core.data.store.table.row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.knime.core.data.store.table.column.ReadableColumnIterator;

public final class ReadableRowIterator implements Iterator<Row<ReadableValueAccess>>, AutoCloseable {

	private final ColumnBackedReadableRow m_row;

	public ReadableRowIterator(final List<ReadableColumnIterator> columns) {
		m_row = new ColumnBackedReadableRow(columns);
	}

	@Override
	public boolean hasNext() {
		return m_row.canFwd();
	}

	@Override
	public Row<ReadableValueAccess> next() {
		m_row.fwd();
		return m_row;
	}

	@Override
	public void close() throws Exception {
		m_row.close();
	}

	private static final class ColumnBackedReadableRow implements Row<ReadableValueAccess>, AutoCloseable {

		private final List<ReadableColumnIterator> m_columns;

		private final List<ReadableValueAccess> m_dataValues;

		public ColumnBackedReadableRow(final List<ReadableColumnIterator> columns) {
			m_columns = columns;
			m_dataValues = new ArrayList<>(columns.size());
			for (final ReadableColumnIterator column : m_columns) {
				m_dataValues.add(column.get());
			}
		}

		private boolean canFwd() {
			return !m_columns.isEmpty() && m_columns.get(0).canFwd();
		}

		private void fwd() {
			for (final ReadableColumnIterator column : m_columns) {
				column.fwd();
			}
		}

		@Override
		public long getNumValues() {
			return m_dataValues.size();
		}

		@Override
		public ReadableValueAccess getValueAt(final int idx) {
			return m_dataValues.get(idx);
		}

		@Override
		public void close() throws Exception {
			for (final ReadableColumnIterator column : m_columns) {
				column.close();
			}
		}
	}
}
