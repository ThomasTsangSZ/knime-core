
package org.knime.core.data.store.column;

import org.knime.core.data.store.column.value.ReadableValueAccess;

public interface ReadableColumnCursor extends AutoCloseable {

	boolean canFwd();

	void fwd();
	
	ReadableValueAccess getValueAccess();
}
