
package org.knime.core.data.store.knime;

import java.util.ArrayList;
import java.util.List;

import org.knime.core.data.store.table.column.ReadableColumnIterator;
import org.knime.core.data.store.table.column.ReadableTable;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.column.WritableTable;
import org.knime.core.data.store.table.row.ReadableRowIterator;
import org.knime.core.data.store.table.row.WritableRowIterator;

public final class TableFactory {

	private TableFactory() {}

	public static WritableRowIterator createRowWriteAccess(final WritableTable table) {
		final List<WritableColumn> columns = new ArrayList<>(Math.toIntExact(table.getNumColumns()));

		for (long i = 0; i < table.getNumColumns(); i++) {
			columns.add(table.getColumnAt(i));
		}
		return new WritableRowIterator(columns);
	}

	public static ReadableRowIterator createRowReadAccess(final ReadableTable table) {
		final List<ReadableColumnIterator> columns = new ArrayList<>(Math.toIntExact(table.getNumColumns()));
		for (long i = 0; i < table.getNumColumns(); i++) {
			columns.add(table.iterator(i));
		}
		return new ReadableRowIterator(columns);
	}
}
