package org.knime.core.data.partition;

import java.io.IOException;

public interface PartitionStore<T> extends AutoCloseable {

	// create linked value
	PartitionValue<T> createLinkedValue();

	// create a new partition. not managed by store.
	Partition<T> createPartition();

	// get partition at index
	Partition<T> get(long index) throws IOException;

	// number managed partition
	long getNumPartitions();
}
