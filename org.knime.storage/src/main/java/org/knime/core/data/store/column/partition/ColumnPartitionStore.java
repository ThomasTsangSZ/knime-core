package org.knime.core.data.store.column.partition;

import java.io.IOException;

public interface ColumnPartitionStore<T> extends AutoCloseable, Iterable<ColumnPartition<T>> {

	@Override
	ColumnPartitionIterator<T> iterator();

	long getNumPartitions();

	void persist(ColumnPartition<T> partition) throws IOException;

	ColumnPartition<T> appendPartition();

	ColumnPartitionValueAccess<T> createAccess();

}
