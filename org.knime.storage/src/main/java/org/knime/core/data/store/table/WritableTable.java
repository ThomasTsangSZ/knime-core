
package org.knime.core.data.store.table;

import org.knime.core.data.store.column.WritableColumn;

public interface WritableTable extends Table, AutoCloseable {

	WritableColumn getWritableColumn(long columnIndex);
}
