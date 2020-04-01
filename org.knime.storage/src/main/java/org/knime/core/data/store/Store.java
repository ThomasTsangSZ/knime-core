package org.knime.core.data.store;

import org.knime.core.data.store.column.ColumnType;
import org.knime.core.data.store.column.partition.ColumnPartitionStore;

public interface Store extends AutoCloseable{

	ColumnPartitionStore<?> create(ColumnType type);

}
