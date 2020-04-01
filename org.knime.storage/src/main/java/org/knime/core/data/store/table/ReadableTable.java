
package org.knime.core.data.store.table;

import org.knime.core.data.store.column.ReadableColumn;

public interface ReadableTable extends Table {

	ReadableColumn getReadableColumn(long columnIndex);
}
