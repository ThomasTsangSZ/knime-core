
package org.knime.core.data.store.table.column;

import java.util.ArrayList;
import java.util.List;

public final class DefaultReadableTable implements ReadableTable {

	// TODO support long lists
	final List<ReadableColumn> m_columns = new ArrayList<>();

	public DefaultReadableTable(List<ReadableColumn> columns) {
		throw new IllegalStateException("not yet implemented");
	}

	@Override
	public long getNumColumns() {
		return m_columns.size();
	}

	@Override
	public ReadableColumnIterator iterator(final long index) {
		// TODO support long lists
		return m_columns.get((int) index).iterator();
	}
}
