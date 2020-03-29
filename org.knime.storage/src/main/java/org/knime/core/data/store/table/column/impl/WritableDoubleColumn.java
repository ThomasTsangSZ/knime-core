
package org.knime.core.data.store.table.column.impl;

import org.knime.core.data.store.table.column.WritableColumn;

public interface WritableDoubleColumn extends WritableColumn {

	void setDoubleValue(final double value);
}
