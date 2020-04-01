package org.knime.core.data.store.column.partition;

public interface ColumnPartition<T> extends AutoCloseable {

	T getStorage();

	long getCapacity();

	long getNumValues();

	// TODO I'd really like to not need that
	void setNumValues(long numValues);
}
