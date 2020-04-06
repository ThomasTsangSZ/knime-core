
package org.knime.core.data.table.column;

import org.knime.core.data.table.value.ReadableValue;

public interface ReadableColumnCursor extends AutoCloseable {

	boolean canFwd();

	void fwd();
	
	ReadableValue getValue();
}
