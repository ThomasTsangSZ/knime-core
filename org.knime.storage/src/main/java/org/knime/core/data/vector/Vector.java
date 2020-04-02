package org.knime.core.data.vector;

// TODO Naming
public interface Vector<T> extends AutoCloseable {

	T get();

	long getCapacity();
	
	void setNumValues(int numValues);

	int getNumValues();
}
