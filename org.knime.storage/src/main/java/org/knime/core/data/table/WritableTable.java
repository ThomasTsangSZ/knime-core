
package org.knime.core.data.table;

import org.knime.core.data.table.column.WritableColumn;

public interface WritableTable {

	long getNumColumns();

	WritableColumn getWritableColumn(long columnIndex);
}
