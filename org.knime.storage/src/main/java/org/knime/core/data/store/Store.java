package org.knime.core.data.store;

import org.knime.core.data.store.partition.ColumnPartitionStore;
import org.knime.core.data.store.table.column.ColumnType;

public interface Store extends AutoCloseable{

	ColumnPartitionStore<?> create(ColumnType type);

}
