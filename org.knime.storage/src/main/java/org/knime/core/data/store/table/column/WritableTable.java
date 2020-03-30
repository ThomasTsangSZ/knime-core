
package org.knime.core.data.store.table.column;

public interface WritableTable extends AutoCloseable {

	long getNumColumns();

	WritableColumn getColumnAt(long columnIndex);
}
