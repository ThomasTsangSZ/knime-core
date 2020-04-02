
package org.knime.core.data.table;

import org.knime.core.data.table.column.WritableColumnCursor;

public interface WritableTable extends AutoCloseable {

	long getNumColumns();

	WritableColumnCursor getWritableColumnCursor(long columnIndex);
}
