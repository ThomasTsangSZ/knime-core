package org.knime.core.data.store.partition;

public interface ColumnPartitionValueAccess {
	void incIndex();

	void updateBufferAccess(ColumnPartition bufferAccess);
}