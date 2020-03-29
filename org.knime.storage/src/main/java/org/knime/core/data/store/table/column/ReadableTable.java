
package org.knime.core.data.store.table.column;

public interface ReadableTable extends AutoCloseable {

	long getNumColumns();

	ReadableColumn getColumnAt(long index);
}
