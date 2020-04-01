
package org.knime.core.data.store.table;

import org.knime.core.data.store.column.WritableColumnCursor;

public interface WritableTable extends AutoCloseable {

	long getNumColumns();

	WritableColumnCursor getWritableColumnCursor(long columnIndex);
}
