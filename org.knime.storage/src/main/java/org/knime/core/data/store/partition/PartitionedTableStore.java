package org.knime.core.data.store.partition;

// TODO Iterator?
public interface PartitionedTableStore {

	ColumnPartitionStore getOrCreate(long index);

	long numStoredColumns();
}
