
package org.knime.core.data.table.row;

import org.knime.core.data.table.value.WritableValue;

public interface WritableRow extends AutoCloseable {

	void fwd();

	long getNumValueAccesses();

	WritableValue getValueAccessAt(int index);
}
