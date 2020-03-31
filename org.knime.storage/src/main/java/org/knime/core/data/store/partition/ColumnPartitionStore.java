package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.column.ColumnType;

public interface ColumnPartitionStore extends AutoCloseable {
	long getNumPartitions();

	ColumnType getColumnType();

	ColumnPartition getOrCreatePartition(long partitionIndex);

	ColumnPartitionReadableValueAccess getReadAccess();

	ColumnPartitionWritableValueAccess getWriteAccess();

	// Release memory
	@Override
	default void close() throws Exception {
		for (long i = 0; i < getNumPartitions(); i++) {
			getOrCreatePartition(i).close();
		}
	};
}
