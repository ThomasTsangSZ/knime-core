package org.knime.core.data.store.partition;

public interface ColumnPartition<T> extends AutoCloseable {

	T get();

	long getPartitionIndex();

	int getCapacity();

	int getNumValues();

	// TODO I'd really like to not need that
	void setNumValues(int numValues);
}
