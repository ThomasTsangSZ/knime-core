
package org.knime.core.data.store.table.row;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.column.WritableTable;
import org.knime.core.data.store.table.value.WritableValueAccess;

// TODO: Implemented against KNIME classes ('DataValue', 'DataCell', ...)
public final class ColumnBackedWritableRow implements WritableRow {

	public static ColumnBackedWritableRow fromWritableTable(final WritableTable table) {
		final List<WritableColumn> columns = new ArrayList<>(Math.toIntExact(table.getNumColumns()));
		for (long i = 0; i < table.getNumColumns(); i++) {
			columns.add(table.getColumnAt(i));
		}
		return new ColumnBackedWritableRow(columns);
	}

	private final List<WritableColumn> m_columns;

	private final List<WritableValueAccess> m_dataValues;

	public ColumnBackedWritableRow(final List<WritableColumn> columns) {
		m_columns = columns;
		m_dataValues = new ArrayList<>(columns.size());
		for (final WritableColumn column : m_columns) {
			m_dataValues.add(column.getValueAccess());
		}
	}

	@Override
	public long getNumValueAccesses() {
		return m_dataValues.size();
	}

	@Override
	public void fwd() {
		for (final WritableColumn column : m_columns) {
			column.fwd();
		}
	}

	@Override
	public WritableValueAccess getValueAccessAt(final int idx) {
		return m_dataValues.get(idx);
	}

	@Override
	public void close() throws Exception {
		for (final WritableColumn column : m_columns) {
			column.close();
		}
	}
}
