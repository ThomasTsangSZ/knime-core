package org.knime.core.data.store.partition;

import java.io.IOException;

public interface ColumnPartitionStore<T> extends AutoCloseable {

	long getNumPartitions();

	void persist(long partitionIndex) throws IOException;

	ColumnPartition<T> getOrCreatePartition(long partitionIndex);

	ColumnPartitionReadableValueAccess<T> createLinkedReadAccess();

	ColumnPartitionWritableValueAccess<T> createLinkedWriteAccess();

}
