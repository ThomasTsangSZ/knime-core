package org.knime.core.data.store.partition;

public interface ColumnPartitionValueAccess {
	void incIndex();

	void updatePartition(ColumnPartition partition);
}