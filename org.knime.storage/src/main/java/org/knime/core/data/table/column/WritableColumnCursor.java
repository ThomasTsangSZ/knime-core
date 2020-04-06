
package org.knime.core.data.table.column;

import org.knime.core.data.table.value.WritableValue;

public interface WritableColumnCursor extends AutoCloseable {

	void fwd();

	WritableValue getValue();
}
