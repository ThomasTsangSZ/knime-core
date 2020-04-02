package org.knime.core.data.store;

import java.io.IOException;

import org.knime.core.data.table.ReadableTable;
import org.knime.core.data.table.column.Partition;
import org.knime.core.data.table.column.PartitionValue;
import org.knime.core.data.table.column.ReadableColumn;
import org.knime.core.data.table.column.ReadableColumnCursor;
import org.knime.core.data.table.column.ReadablePartitionedColumn;
import org.knime.core.data.table.column.ReadablePartitionedColumnCursor;

// TODO common super class with 'StoreBackedWritable'
public class StoreBackedReadableTable implements ReadableTable {

	private ReadableColumn[] m_column;

	public StoreBackedReadableTable(RootStore store) {
		m_column = new ReadableColumn[(int) store.getNumStores()];
		for (int i = 0; i < m_column.length; i++) {
			m_column[i] = new DefaultWritablePartitionedColumn<>(store.getStoreAt(i));
//			else {
//				@SuppressWarnings("unchecked")
//				final WritablePartitionedColumn<Long, ?>[] struct = new WritablePartitionedColumn[types.length];
//				for (int i = 0; i < struct.length; i++) {
//					struct[i] = createGroupForType(types[i]);
//				}
//				storeForType = new WritableVectorStoreGroup<>(struct);
//			}
		}

	}

	@Override
	public ReadableColumn getReadableColumn(long columnIndex) {
		return m_column[(int) columnIndex];
	}

	@Override
	public long getNumColumns() {
		return m_column.length;
	}

	class DefaultWritablePartitionedColumn<D> implements ReadablePartitionedColumn<D>, ReadableColumn {

		private final Store<D> m_store;

		public DefaultWritablePartitionedColumn(Store<D> store) {
			m_store = store;
		}

		@Override
		public PartitionValue<D> createLinkedValue() {
			return m_store.createLinkedValue();
		}

		@Override
		public Partition<D> get(long idx) throws IOException {
			return m_store.get(idx);
		}

		@Override
		public long getNumPartitions() {
			return m_store.getPartitions();
		}

		@Override
		public ReadableColumnCursor createCursor() {
			// TODO we do need some kind of ref-counting here in order to not destroy
			// something before we're allowed to.
			return new ReadablePartitionedColumnCursor<D>(this);
		}

	}
}
