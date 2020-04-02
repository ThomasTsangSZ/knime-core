package org.knime.core.data.table.column;

public interface Partition<T> extends AutoCloseable {

	T get();

	long getCapacity();

	// TODO can we get rid of this?
	void setNumValues(int numValues);

	int getNumValues();
}
