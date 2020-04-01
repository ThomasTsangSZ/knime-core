
package org.knime.core.data.store.table;

import org.knime.core.data.store.column.ReadableColumn;

public interface ReadableTable {

	long getNumColumns();

	ReadableColumn getReadableColumn(long columnIndex);
}
