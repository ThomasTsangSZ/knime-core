package org.knime.core.data.arrow.struct;

import java.io.IOException;

import org.knime.core.data.arrow.ArrowStore;
import org.knime.core.data.table.column.Partition;
import org.knime.core.data.table.column.PartitionValue;

public class StructArrowStore implements ArrowStore<Partition<?>[]> {

	private ArrowStore<?>[] m_stores;

	public StructArrowStore(ArrowStore<?>... stores) {
		m_stores = stores;
	}

	@Override
	public PartitionValue<Partition<?>[]> createLinkedValue() {
		final PartitionValue<?>[] values = new PartitionValue[m_stores.length];
		for (int i = 0; i < values.length; i++) {
			values[i] = m_stores[i].createLinkedValue();
		}
		return new StructArrowPartitionValue(values, m_stores);
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

}
