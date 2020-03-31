package org.knime.core.data.store.partition;

public interface ColumnPartition<T> extends AutoCloseable {

	T get();

	int getValueCount();

	int getValueCapacity();

}
