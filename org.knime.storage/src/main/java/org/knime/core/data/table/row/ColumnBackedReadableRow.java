
package org.knime.core.data.table.row;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.table.ReadableTable;
import org.knime.core.data.table.column.ReadableColumnCursor;
import org.knime.core.data.table.value.ReadableValue;

//TODO: Implemented against KNIME classes ('DataValue', 'DataCell', ...)
public final class ColumnBackedReadableRow implements ReadableRow {

	public static ColumnBackedReadableRow fromReadableTable(final ReadableTable table) {
		final List<ReadableColumnCursor> columns = new ArrayList<>(Math.toIntExact(table.getNumColumns()));
		for (long i = 0; i < table.getNumColumns(); i++) {
			columns.add(table.getReadableColumn(i).createCursor());
		}
		return new ColumnBackedReadableRow(columns);
	}

	private final List<ReadableColumnCursor> m_columns;

	private final List<ReadableValue> m_dataValues;

	public ColumnBackedReadableRow(final List<ReadableColumnCursor> columns) {
		m_columns = columns;
		m_dataValues = new ArrayList<>(columns.size());
		for (final ReadableColumnCursor column : m_columns) {
			m_dataValues.add(column.getValueAccess());
		}
	}

	@Override
	public long getNumValueAccesses() {
		return m_dataValues.size();
	}

	@Override
	public boolean canFwd() {
		return !m_columns.isEmpty() && m_columns.get(0).canFwd();
	}

	@Override
	public void fwd() {
		for (final ReadableColumnCursor column : m_columns) {
			column.fwd();
		}
	}

	@Override
	public ReadableValue getValueAccessAt(final int idx) {
		return m_dataValues.get(idx);
	}

	@Override
	public void close() throws Exception {
		for (final ReadableColumnCursor column : m_columns) {
			column.close();
		}
	}
}