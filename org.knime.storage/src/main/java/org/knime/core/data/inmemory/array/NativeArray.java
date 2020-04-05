package org.knime.core.data.inmemory.array;

public interface NativeArray<A> extends AutoCloseable {
	boolean isMissing(int index);

	void setMissing(int index);

	A get();

	long size();
}
