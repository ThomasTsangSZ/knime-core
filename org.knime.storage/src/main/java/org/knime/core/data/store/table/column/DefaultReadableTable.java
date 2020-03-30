
package org.knime.core.data.store.table.column;

import org.knime.core.data.store.Store;

public final class DefaultReadableTable implements ReadableTable {

	private final Store m_store;

	public DefaultReadableTable(final Store store) {
		m_store = store;
	}

	@Override
	public long getNumColumns() {
		return m_store.getNumLogicalColumns();
	}

	@Override
	public ReadableColumnIterator iterator(final long index) {
		return m_store.getReadableLogicalColumnAt(index).iterator();
	}
}
