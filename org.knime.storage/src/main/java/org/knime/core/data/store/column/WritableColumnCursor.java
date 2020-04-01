
package org.knime.core.data.store.column;

import org.knime.core.data.store.column.value.WritableValueAccess;

public interface WritableColumnCursor extends AutoCloseable {

	void fwd();

	WritableValueAccess getValueAccess();
}
