package org.knime.core.data.store;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.knime.core.data.table.WritableTable;
import org.knime.core.data.table.column.Partition;
import org.knime.core.data.table.column.PartitionValue;
import org.knime.core.data.table.column.WritableColumnCursor;
import org.knime.core.data.table.column.WritablePartitionedColumn;
import org.knime.core.data.table.column.WritablePartitionedColumnCursor;

public class StoreBackedWritableTable implements WritableTable {

	private WritablePartitionedColumnCursor<?>[] m_cursors;

	public StoreBackedWritableTable(RootStore root) {
		m_cursors = new WritablePartitionedColumnCursor[(int) root.getNumStores()];
		for (long i = 0; i < root.getNumStores(); i++) {
			m_cursors[(int) i] = new WritablePartitionedColumnCursor<>(
					new DefaultWritablePartitionedColumn<>(root.getStoreAt(i)));
		}

	}

	@Override
	public long getNumColumns() {
		return m_cursors.length;
	}

	@Override
	public WritableColumnCursor getWritableColumnCursor(long columnIndex) {
		return m_cursors[(int) columnIndex];
	}

	class DefaultWritablePartitionedColumn<D> implements WritablePartitionedColumn<D> {

		private long m_numVectors;
		private Store<D> m_store;

		DefaultWritablePartitionedColumn(Store<D> store) {
			m_store = store;
		}

		@Override
		public PartitionValue<D> createLinkedValue() {
			return m_store.createLinkedValue();
		}

		@Override
		public Pair<Long, Partition<D>> extend() {
			final Partition<D> partition = m_store.createPartition();
			return new ImmutablePair<Long, Partition<D>>(m_numVectors++, new Partition<D>() {

				@Override
				public D get() {
					return partition.get();
				}

				@Override
				public long getCapacity() {
					return partition.getCapacity();
				}

				@Override
				public void setNumValues(int numValues) {
					partition.setNumValues(numValues);
				}

				@Override
				public int getNumValues() {
					return partition.getNumValues();
				}

				@Override
				public void close() throws Exception {
					// add back to store after writing
					m_store.addPartition(partition);
				}
			});
		}
	}
}
