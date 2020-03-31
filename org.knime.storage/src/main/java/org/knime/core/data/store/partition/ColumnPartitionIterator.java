package org.knime.core.data.store.partition;

import java.util.Iterator;

public interface ColumnPartitionIterator<T> extends Iterator<ColumnPartition<T>> {
	// reads can be expensive. therefore skip.
	// TODO maybe we get rid of this by introducing lazyness to next. for now we
	// leave it like this.
	void skip();
}
