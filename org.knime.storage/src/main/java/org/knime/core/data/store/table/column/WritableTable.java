
package org.knime.core.data.store.table.column;

public interface WritableTable extends Table, AutoCloseable {

	WritableColumn getWritableColumn(long columnIndex);
}
