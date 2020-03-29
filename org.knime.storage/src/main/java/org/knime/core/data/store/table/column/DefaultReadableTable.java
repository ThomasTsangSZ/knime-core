
package org.knime.core.data.store.table.column;

public final class DefaultReadableTable implements ReadableTable {

	private final ReadableColumn[] m_columns;

	public DefaultReadableTable(/* TODO: Pass some table store here? */) {
		throw new IllegalStateException("not yet implemented");
	}

	@Override
	public long getNumColumns() {
		return m_columns.length;
	}

	@Override
	public ReadableColumn getColumnAt(final long index) {
		return m_columns[Math.toIntExact(index)];
	}

	@Override
	public void close() throws Exception {
		for (final ReadableColumn column : m_columns) {
			column.close();
		}
	}
}
