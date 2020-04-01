package org.knime.core.data.store.arrow;

import org.knime.core.data.store.column.partition.ColumnPartitionFactory;
import org.knime.core.data.store.column.partition.ColumnPartitionReader;
import org.knime.core.data.store.column.partition.PartitionedColumnValueAccess;
import org.knime.core.data.store.column.partition.ColumnPartitionWriter;

// Actually providing access to data of a column
// TODO restructure? rename?
public interface ArrowColumnAccess<T> extends ColumnPartitionFactory<T>, ColumnPartitionWriter<T> {

	PartitionedColumnValueAccess<T> createLinkedType();

	ColumnPartitionReader<T> create();

	void destroy() throws Exception;

	long getNumPartitions();

}
