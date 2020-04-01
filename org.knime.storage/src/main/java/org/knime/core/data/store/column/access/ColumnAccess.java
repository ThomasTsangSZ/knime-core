package org.knime.core.data.store.column.access;

import org.knime.core.data.store.column.partition.ColumnPartitionFactory;
import org.knime.core.data.store.column.partition.ColumnPartitionReader;
import org.knime.core.data.store.column.partition.ColumnPartitionValueAccess;
import org.knime.core.data.store.column.partition.ColumnPartitionWriter;

// Actually providing access to data of a column
// TODO restructure? rename?
public interface ColumnAccess<T> extends ColumnPartitionFactory<T>, ColumnPartitionWriter<T> {

	ColumnPartitionValueAccess<T> createLinkedType();

	ColumnPartitionReader<T> create();

	void destroy() throws Exception;

	long getNumPartitions();

}
