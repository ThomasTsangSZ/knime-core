package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.column.ColumnType;

// TODO Iterator?
public interface TablePartitionStore {

	ColumnPartitionStore add(ColumnType type);

	long numStoredColumns();
}
