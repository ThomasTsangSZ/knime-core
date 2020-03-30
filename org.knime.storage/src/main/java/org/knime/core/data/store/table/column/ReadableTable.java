
package org.knime.core.data.store.table.column;

public interface ReadableTable {

	long getNumColumns();

	ReadableColumnIterator iterator(long columnIndex);
}
