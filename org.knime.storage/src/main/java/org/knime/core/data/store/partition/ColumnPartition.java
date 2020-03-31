package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.column.ColumnType;

public interface ColumnPartition extends AutoCloseable {

	ColumnType getType();

	int getValueCount();

	int getValueCapacity();

}
