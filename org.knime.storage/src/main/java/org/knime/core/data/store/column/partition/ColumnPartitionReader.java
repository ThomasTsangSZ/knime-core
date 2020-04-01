package org.knime.core.data.store.column.partition;

import java.util.Iterator;

public interface ColumnPartitionReader<T> extends AutoCloseable, Iterator<ColumnPartition<T>> {
	void skip();
}
