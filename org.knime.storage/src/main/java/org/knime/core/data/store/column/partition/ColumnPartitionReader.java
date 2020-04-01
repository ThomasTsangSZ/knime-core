package org.knime.core.data.store.column.partition;

public interface ColumnPartitionReader<T> extends AutoCloseable {
	ColumnPartition<T> readNext();

	boolean hasNext();
	
	void skip();
}
