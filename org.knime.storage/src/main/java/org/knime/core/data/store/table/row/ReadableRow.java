
package org.knime.core.data.store.table.row;

import org.knime.core.data.store.column.value.ReadableValueAccess;

public interface ReadableRow extends AutoCloseable {

	boolean canFwd();

	void fwd();

	long getNumValueAccesses();

	ReadableValueAccess getValueAccessAt(int index);
}
