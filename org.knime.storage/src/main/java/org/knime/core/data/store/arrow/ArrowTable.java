package org.knime.core.data.store.arrow;

import java.io.IOException;

import org.knime.core.data.store.Store;
import org.knime.core.data.store.column.ColumnSchema;
import org.knime.core.data.store.column.ReadableColumn;
import org.knime.core.data.store.column.ReadableColumnCursor;
import org.knime.core.data.store.column.WritableColumn;
import org.knime.core.data.store.column.partition.CachedColumnPartitionStore;
import org.knime.core.data.store.column.partition.ColumnPartitionStore;
import org.knime.core.data.store.column.partition.PartitionedReadableColumnCursor;
import org.knime.core.data.store.column.partition.PartitionedWritableColumn;
import org.knime.core.data.store.table.ReadableTable;
import org.knime.core.data.store.table.WritableTable;

public class ArrowTable implements ReadableTable, WritableTable {

	// TODO we support 'Long'-many columns.
	private final ColumnPartitionStore<?>[] m_columnPartitionStores;
	private final PartitionedWritableColumn<?>[] m_writableColumn;

	private final Store m_store;

	public ArrowTable(final ColumnSchema[] schema, final ArrowStore store) throws IOException {
		m_store = store;
		m_columnPartitionStores = new ColumnPartitionStore[schema.length];
		m_writableColumn = new PartitionedWritableColumn[schema.length];
		for (int i = 0; i < schema.length; i++) {
			// TODO we can do caching here... or ... somewhere else
			m_columnPartitionStores[i] = new CachedColumnPartitionStore<>(m_store.create(schema[(int) i].getType()));
			@SuppressWarnings("resource")
			PartitionedWritableColumn<?> writable = new PartitionedWritableColumn<>(m_columnPartitionStores[i]);
			m_writableColumn[i] = writable;
		}
	}

	@Override
	public ReadableColumn getReadableColumn(long columnIndex) {
		return new ReadableColumn() {
			@Override
			public ReadableColumnCursor cursor() {
				return new PartitionedReadableColumnCursor<>(m_columnPartitionStores[(int) columnIndex]);
			}
		};
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
