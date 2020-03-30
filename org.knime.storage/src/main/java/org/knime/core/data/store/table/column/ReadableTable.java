
package org.knime.core.data.store.table.column;

public interface ReadableTable {

	long getNumColumns();

	ReadableColumnIterator getColumnIteratorAt(long columnIndex);
}
