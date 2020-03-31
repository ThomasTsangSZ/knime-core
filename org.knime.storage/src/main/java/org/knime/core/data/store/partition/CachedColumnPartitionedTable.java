package org.knime.core.data.store.partition;

import java.io.IOException;

import org.knime.core.data.store.Store;
import org.knime.core.data.store.table.column.ColumnSchema;
import org.knime.core.data.store.table.column.ReadableColumnCursor;
import org.knime.core.data.store.table.column.ReadableTable;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.column.WritableTable;

public class CachedColumnPartitionedTable implements ReadableTable, WritableTable {

	// TODO we support 'Long'-many columns.
	private final ColumnPartitionStore<?>[] m_columnPartitionStores;
	private final WritablePartitionedColumn<?>[] m_writableColumn;

	private final Store m_store;

	public CachedColumnPartitionedTable(final ColumnSchema[] schema, final Store store) throws IOException {
		m_store = store;
		m_columnPartitionStores = new ColumnPartitionStore[schema.length];
		m_writableColumn = new WritablePartitionedColumn[schema.length];
		for (int i = 0; i < schema.length; i++) {
			// TODO we can do caching here... or ... somewhere else
			m_columnPartitionStores[i] = new CachedColumnPartitionStore<>(m_store.create(schema[(int) i].getType()));
			@SuppressWarnings("resource")
			WritablePartitionedColumn<?> writable = new WritablePartitionedColumn<>(m_columnPartitionStores[i]);
			m_writableColumn[i] = writable;
		}
	}

	@Override
	public ReadableColumnCursor createReadableColumnCursor(long columnIndex) {
		final ReadablePartitionedColumnCursor<?> cursor = new ReadablePartitionedColumnCursor<>(
				m_columnPartitionStores[(int) columnIndex]);
		return cursor;
	}

	@Override
	public WritableColumn getWritableColumn(long columnIndex) {
		return m_writableColumn[(int) columnIndex];
	}

	@Override
	public long getNumColumns() {
		return m_columnPartitionStores.length;
	}

	@Override
	public void close() throws Exception {
		// TODO we have to check if someone still has a reference on this column?
		for (ColumnPartitionStore<?> store : m_columnPartitionStores) {
			store.close();
		}

		m_store.close();

		// TODO i'm wouldn't know what 'close()' means for this table
		// 'close()' -> release memory
		// 'destroy()' delete any trace of this table
		// NB: We don't need 'closeForWriting()'. Design allows to have concurrent
		// read/write (e.g. for streaming)
	}

}
