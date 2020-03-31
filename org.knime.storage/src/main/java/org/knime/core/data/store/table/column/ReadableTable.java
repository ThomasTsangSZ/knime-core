
package org.knime.core.data.store.table.column;

public interface ReadableTable extends Table {

	ReadableColumnCursor createReadableColumnCursor(long columnIndex);
}
