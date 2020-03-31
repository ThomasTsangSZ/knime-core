package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.column.ColumnType;

public interface ColumnPartition extends AutoCloseable {

	ColumnType getType();

	int getValueCount();

	int getValueCapacity();

	void persist();

	// entirely kill buffer and all traces
	void destroy();

	// only close in-memory representation, however, keep disc if buffer was
	// written.
	@Override
	void close();
}
