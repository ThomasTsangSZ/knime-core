package org.knime.core.data.store.partition;

import java.io.IOException;

public interface ColumnPartitionStore extends AutoCloseable {

	long getNumPartitions();

	void persist(long partitionIndex) throws IOException;

	ColumnPartition getOrCreatePartition(long partitionIndex);

	// Release memory
	@Override
	default void close() throws Exception {
		for (long i = 0; i < getNumPartitions(); i++) {
			getOrCreatePartition(i).close();
		}
	};
}
