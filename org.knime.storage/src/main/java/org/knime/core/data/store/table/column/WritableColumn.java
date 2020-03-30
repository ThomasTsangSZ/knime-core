
package org.knime.core.data.store.table.column;

import org.knime.core.data.store.table.row.WritableValueAccess;

public interface WritableColumn {

	void fwd();

	WritableValueAccess get();
}
