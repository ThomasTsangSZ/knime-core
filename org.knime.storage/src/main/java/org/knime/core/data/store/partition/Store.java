package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.column.ColumnType;

public interface Store<T> {

	ColumnPartitionStore<T> create(ColumnType type);

}
