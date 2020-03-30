
package org.knime.core.data.store;

import org.knime.core.data.store.table.column.ReadableColumn;
import org.knime.core.data.store.table.column.WritableColumn;

public interface Store {

	long getNumLogicalColumns();

	WritableColumn getWritableLogicalColumnAt(long index);

	ReadableColumn getReadableLogicalColumnAt(long index);
}
