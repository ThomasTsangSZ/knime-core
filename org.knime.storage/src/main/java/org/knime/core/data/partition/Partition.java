package org.knime.core.data.partition;

public interface Partition<T> extends AutoCloseable {

	long getIndex();
	
	T get();

	long getCapacity();

	// TODO can we get rid of this?
	void setNumValues(int numValues);

	int getNumValues();
}
