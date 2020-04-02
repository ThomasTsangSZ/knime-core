package org.knime.core.data.arrow;

import java.io.IOException;
import java.util.function.Supplier;

import org.knime.core.data.cache.SequentialCache;
import org.knime.core.data.store.Store;
import org.knime.core.data.table.column.Partition;
import org.knime.core.data.table.column.PartitionValue;

class ArrowStore<T> implements Store<T> {

	private final Supplier<PartitionValue<T>> m_valueSupplier;
	private final Supplier<Partition<T>> m_partitionSupplier;
	private final SequentialCache<T> m_cache;

	private long m_partitionIdx;

	public ArrowStore(Supplier<PartitionValue<T>> partitionValueSupplier, Supplier<Partition<T>> partitionSupplier,
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
		return m_partitionSupplier.get();
	}

	@Override
	public void addPartition(Partition<T> partition) {
		m_partitionIdx++;
		m_cache.add(partition);
	}

	@Override
	public Partition<T> get(long index) throws IOException {
		return m_cache.get(index);
	}

	@Override
	public long numPartitions() {
		return m_partitionIdx;
	}

	public void flush() throws Exception {
		// TODO is there a difference between 'flush()' and 'releaseMemory'
		m_cache.flush();
		m_cache.clear();
	}

	// release all memory
	@Override
	public void close() throws Exception {
		m_cache.clear();
	}

}