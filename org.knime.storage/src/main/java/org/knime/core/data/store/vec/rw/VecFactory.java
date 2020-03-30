
package org.knime.core.data.store.vec.rw;

import org.knime.core.data.store.table.column.ColumnType;
import org.knime.core.data.store.vec.VecAccessible;

public interface VecFactory {

	VecAccessible create(ColumnType type);
}
