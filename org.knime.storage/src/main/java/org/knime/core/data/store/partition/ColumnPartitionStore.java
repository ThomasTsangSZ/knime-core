package org.knime.core.data.store.partition;

public interface ColumnPartitionStore extends AutoCloseable {
	long getNumPartitions();

	ColumnPartition getOrCreatePartition(long partitionIndex);

	// Release memory
	@Override
	default void close() throws Exception {
		for (long i = 0; i < getNumPartitions(); i++) {
			getOrCreatePartition(i).close();
		}
	};
}
