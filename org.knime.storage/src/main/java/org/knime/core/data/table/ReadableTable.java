
package org.knime.core.data.table;

import org.knime.core.data.table.column.ReadableColumn;

public interface ReadableTable {

	long getNumColumns();

	ReadableColumn getReadableColumn(long columnIndex);
}
