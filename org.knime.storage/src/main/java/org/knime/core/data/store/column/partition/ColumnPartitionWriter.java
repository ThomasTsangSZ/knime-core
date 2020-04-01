package org.knime.core.data.store.column.partition;

import java.io.IOException;

public interface ColumnPartitionWriter<T> extends AutoCloseable {
	void write(ColumnPartition<T> partition) throws IOException;
}
