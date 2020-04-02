package org.knime.core.data.arrow;

import java.io.IOException;

import org.knime.core.data.table.column.Partition;
import org.knime.core.data.table.column.PartitionValue;
import org.knime.core.data.table.value.ReadableStructValue;
import org.knime.core.data.table.value.ReadableValue;
import org.knime.core.data.table.value.WritableStructValue;
import org.knime.core.data.table.value.WritableValue;

public class ArrowStoreGroup implements ArrowStore<Partition<?>[]> {

	private ArrowStore<?>[] m_stores;

	public ArrowStoreGroup(ArrowStore<?>... stores) {
		m_stores = stores;
	}

	@Override
	public PartitionValue<Partition<?>[]> createLinkedValue() {
		final PartitionValue<?>[] values = new PartitionValue[m_stores.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = m_stores[i].createLinkedValue();
		}
		return new StructArrowPartitionValue(values);
	}

	@Override
	public Partition<Partition<?>[]> createPartition() {
		Partition<?>[] partitions = new Partition[m_stores.length];
		for (int i = 0; i < m_stores.length; i++) {
			partitions[i] = m_stores[i].createPartition();
		}
		return new StructArrowPartition(partitions);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void addPartition(Partition<Partition<?>[]> partition) {
		for (int i = 0; i < m_stores.length; i++) {
			// TODO is there a better way?
			m_stores[i].addPartition((Partition) partition.get()[i]);
		}
	}

	@Override
	public Partition<Partition<?>[]> get(long index) throws IOException {
		Partition<?>[] partitions = new Partition[m_stores.length];
		for (int i = 0; i < m_stores.length; i++) {
			partitions[i] = m_stores[i].get(index);
		}
		// We could potentially cache this
		return new StructArrowPartition(partitions);
	}

	@Override
	public long getPartitions() {
		return m_stores[0].getPartitions();
	}

	@Override
	public void flush() throws Exception {
		for (int i = 0; i < m_stores.length; i++) {
			m_stores[i].flush();
		}
	}

	@Override
	public void close() throws Exception {
		for (final ArrowStore<?> store : m_stores) {
			store.close();
		}
	}

	class StructArrowPartition implements Partition<Partition<?>[]> {

		private Partition<?>[] m_partitions;

		public StructArrowPartition(Partition<?>[] partitions) {
			m_partitions = partitions;
		}

		@Override
		public void close() throws Exception {
			for (int i = 0; i < m_partitions.length; i++) {
				m_partitions[i].close();
			}
		}

		@Override
		public Partition<?>[] get() {
			return m_partitions;
		}

		@Override
		public long getCapacity() {
			// assumption: all partitions have same capacity
			return m_partitions[0].getCapacity();
		}

		@Override
		public void setNumValues(int numValues) {
			for (int i = 0; i < m_partitions.length; i++) {
				m_partitions[i].setNumValues(numValues);
			}
		}

		@Override
		public int getNumValues() {
			return m_partitions[0].getNumValues();
		}

	}

	class StructArrowPartitionValue
			implements PartitionValue<Partition<?>[]>, ReadableStructValue, WritableStructValue {

		private PartitionValue<?>[] m_values;

		StructArrowPartitionValue(PartitionValue<?>[] values) {
			m_values = values;
		}

		@Override
		public boolean isMissing() {
			boolean isMissing = true;

			// if all is missing.
			for (final PartitionValue<?> value : m_values) {
				isMissing = value.isMissing();
				if (!isMissing) {
					return false;
				}
			}
			return isMissing;
		}

		@Override
		public void setMissing() {
			for (PartitionValue<?> value : m_values) {
				value.setMissing();
			}
		}

		@Override
		public WritableValue writableValueAt(long i) {
			return m_values[(int) i];
		}

		@Override
		public ReadableValue readableValueAt(long i) {
			return m_values[(int) i];
		}

		@Override
		public void incIndex() {
			for (PartitionValue<?> value : m_values) {
				value.incIndex();
			}
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public void updatePartition(Partition<Partition<?>[]> partition) {
			for (int i = 0; i < m_stores.length; i++) {
				// TODO is there a better way?
				m_values[i].updatePartition((Partition) partition.get()[i]);
			}
		}

	}

}
