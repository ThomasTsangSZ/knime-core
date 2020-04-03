package org.knime.core.data.store;

// we could even split this guy into read/write
public interface RootStore extends AutoCloseable {

	// number of created stores.
	long getNumStores();

	Store<?> getStoreAt(long index);

	void flush() throws Exception;

	// delete all traces of this store
	void destroy() throws Exception;
}
