
package org.knime.core.data.store.table.column;

import org.knime.core.data.store.table.row.ReadableValueAccess;

public interface ReadableColumnIterator extends AutoCloseable {

	boolean canFwd();

	void fwd();
	
	ReadableValueAccess get();
}
