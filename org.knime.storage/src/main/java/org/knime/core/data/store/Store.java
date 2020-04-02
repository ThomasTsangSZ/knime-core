package org.knime.core.data.store;

import java.io.IOException;

import org.knime.core.data.table.column.Partition;
import org.knime.core.data.table.column.PartitionValue;

public interface Store<T> extends AutoCloseable {

	// create linked value
	PartitionValue<T> createLinkedValue();

	// create a new partition. not managed by store.
	Partition<T> createPartition();

	// add partition to store.
	void addPartition(Partition<T> partition);

	// get partition at index
	Partition<T> get(long index) throws IOException;

	// number managed partition
	long getPartitions();
}
