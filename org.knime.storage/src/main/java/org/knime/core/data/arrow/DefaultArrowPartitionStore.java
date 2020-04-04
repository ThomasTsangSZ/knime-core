package org.knime.core.data.arrow;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.cache.SequentialCache;
import org.knime.core.data.partition.Partition;
import org.knime.core.data.partition.PartitionValue;

class DefaultArrowPartitionStore<T extends FieldVector> implements ArrowPartitionStore<T> {

	private final Supplier<PartitionValue<T>> m_valueSupplier;
	private final Supplier<T> m_partitionSupplier;
	private final SequentialCache<T> m_cache;
	private long m_partitionIdx;

	public DefaultArrowPartitionStore(Supplier<PartitionValue<T>> partitionValueSupplier, Supplier<T> partitionSupplier,
			SequentialCache<T> cache) {
		m_valueSupplier = partitionValueSupplier;
		m_partitionSupplier = partitionSupplier;
		m_cache = cache;
	}

	@Override
	public PartitionValue<T> createLinkedValue() {
		return m_valueSupplier.get();
	}

	@Override
	public Partition<T> createPartition() {
		T t = m_partitionSupplier.get();
		return new ArrowPartition<T>(t, t.getValueCapacity(), m_partitionIdx++) {
			@Override
			public void close() throws Exception {
				// instead of releasing the reference, we add ourselves in the cache.
				m_cache.add(getIndex(), this);
			}
		};
	}

	@Override
	public Partition<T> get(long index) throws IOException {
		return m_cache.get(index);
	}

	@Override
	public long getNumPartitions() {
		return m_partitionIdx;
	}

	@Override
	public void flush() throws Exception {
		// TODO is there a difference between 'flush' and 'releaseMemory'
		m_cache.flush();
		m_cache.clear();
	}

	// release all memory
	@Override
	public void close() throws Exception {
		m_cache.close();
	}

}