package org.knime.core.data.partition;

// we could even split this guy into read/write
public interface Store extends AutoCloseable {

	// number of created stores.
	long getNumStores();

	PartitionStore<?> getStoreAt(long index);

	void flush() throws Exception;

	// delete all traces of this store
	void destroy() throws Exception;
}
