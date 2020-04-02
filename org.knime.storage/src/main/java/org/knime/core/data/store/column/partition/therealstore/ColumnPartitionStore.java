
package org.knime.core.data.store.column.partition.therealstore;

import java.io.Flushable;

import org.knime.core.data.store.column.partition.ColumnPartitionReader;
import org.knime.core.data.store.column.partition.ColumnPartitionWriter;

public interface ColumnPartitionStore<P> extends Flushable, AutoCloseable {

	ColumnPartitionReader<P> createReader();

	ColumnPartitionWriter<P> getWriter();

	void destroy() throws Exception;
}
