package org.knime.core.data.vector;

public interface Vector<T> extends RefManaged {

	T get();

	long getCapacity();

	// TODO can we get rid of this?
	void setNumValues(int numValues);

	int getNumValues();
}
