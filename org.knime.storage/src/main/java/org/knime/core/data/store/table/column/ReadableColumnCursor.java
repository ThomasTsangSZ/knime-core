
package org.knime.core.data.store.table.column;

import org.knime.core.data.store.table.value.ReadableValueAccess;

public interface ReadableColumnCursor extends AutoCloseable {

	boolean canFwd();

	void fwd();
	
	ReadableValueAccess getValueAccess();
}
