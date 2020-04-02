
package org.knime.core.data.table.row;

import org.knime.core.data.table.value.ReadableValue;

public interface ReadableRow extends AutoCloseable {

	boolean canFwd();

	void fwd();

	long getNumValueAccesses();

	ReadableValue getValueAccessAt(int index);
}
