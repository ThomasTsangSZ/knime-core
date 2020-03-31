
package org.knime.core.data.store.table.column;

import org.knime.core.data.store.table.value.WritableValueAccess;

// TODO I still don't like that 'WritableColumn' is AutoCloseable but 'ReadableColumn' isn't.
public interface WritableColumn extends AutoCloseable {

	void fwd();

	WritableValueAccess getValueAccess();
}
