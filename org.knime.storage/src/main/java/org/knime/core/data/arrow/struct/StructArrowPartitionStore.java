package org.knime.core.data.arrow.struct;

import java.io.IOException;

import org.knime.core.data.arrow.ArrowPartitionStore;
import org.knime.core.data.partition.Partition;
import org.knime.core.data.partition.PartitionValue;

public class StructArrowPartitionStore implements ArrowPartitionStore<Partition<?>[]> {

	private ArrowPartitionStore<?>[] m_stores;
	private long m_index;

	public StructArrowPartitionStore(long index, ArrowPartitionStore<?>... stores) {
		m_stores = stores;
		m_index = index;
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
		return new StructArrowPartition(partitions, m_index);
	}

	@Override
	public Partition<Partition<?>[]> get(long index) throws IOException {
		Partition<?>[] partitions = new Partition[m_stores.length];
		for (int i = 0; i < m_stores.length; i++) {
			partitions[i] = m_stores[i].get(index);
		}
		// We could potentially cache this
		return new StructArrowPartition(partitions, m_index);
	}

	@Override
	public long getNumPartitions() {
		return m_stores[0].getNumPartitions();
	}

	@Override
	public void flush() throws Exception {
		for (int i = 0; i < m_stores.length; i++) {
			m_stores[i].flush();
		}
	}

	@Override
	public void close() throws Exception {
		for (final ArrowPartitionStore<?> store : m_stores) {
			store.close();
		}
	}

}
