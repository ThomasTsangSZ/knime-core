package org.knime.core.data.cache;

import org.knime.core.data.partition.Partition;

public interface RefCountingPartition<T> extends Partition<T> {

	void incRefCount();

}
