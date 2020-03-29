package org.knime.core.data.store;

public interface Access<T> extends AutoCloseable {

	void fwd();

	boolean hasNext();

	T get();

	T next();
}
