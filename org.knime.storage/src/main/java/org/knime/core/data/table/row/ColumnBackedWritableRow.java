
package org.knime.core.data.table.row;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.table.WritableTable;
import org.knime.core.data.table.column.WritableColumnCursor;
import org.knime.core.data.table.value.WritableValue;

// TODO: Implemented against KNIME classes ('DataValue', 'DataCell', ...)
public final class ColumnBackedWritableRow implements WritableRow {

	public static ColumnBackedWritableRow fromWritableTable(final WritableTable table) {
		final List<WritableColumnCursor> columns = new ArrayList<>(Math.toIntExact(table.getNumColumns()));
		for (long i = 0; i < table.getNumColumns(); i++) {
			columns.add(table.getWritableColumn(i).createWritableCursor());
		}
		return new ColumnBackedWritableRow(columns);
	}

	private final List<WritableColumnCursor> m_columns;

	private final List<WritableValue> m_dataValues;

	public ColumnBackedWritableRow(final List<WritableColumnCursor> columns) {
		m_columns = columns;
		m_dataValues = new ArrayList<>(columns.size());
		for (final WritableColumnCursor column : m_columns) {
			m_dataValues.add(column.getValue());
		}
	}

	@Override
	public long getNumValues() {
		return m_dataValues.size();
	}

	@Override
	public void fwd() {
		for (final WritableColumnCursor column : m_columns) {
			column.fwd();
		}
	}

	@Override
	public WritableValue getValueAt(final int idx) {
		return m_dataValues.get(idx);
	}

	@Override
	public void close() throws Exception {
		for (final WritableColumnCursor column : m_columns) {
			column.close();
		}
	}
}
