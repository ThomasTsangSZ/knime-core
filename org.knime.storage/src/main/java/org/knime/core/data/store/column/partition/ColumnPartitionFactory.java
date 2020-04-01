package org.knime.core.data.store.column.partition;

public interface ColumnPartitionFactory<T> {
	ColumnPartition<T> createPartition();
}
