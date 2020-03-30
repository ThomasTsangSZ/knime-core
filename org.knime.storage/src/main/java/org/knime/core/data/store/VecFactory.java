
package org.knime.core.data.store;

import org.knime.core.data.store.table.column.ColumnType;

public interface VecFactory {

	VecAccessible create(ColumnType type);
}
