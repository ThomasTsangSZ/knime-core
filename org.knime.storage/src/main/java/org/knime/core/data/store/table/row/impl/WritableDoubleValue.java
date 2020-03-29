
package org.knime.core.data.store.table.row.impl;

import org.knime.core.data.store.table.column.impl.WritableDoubleColumn;

public final class WritableDoubleValue extends AbstractWritableDataValue<WritableDoubleColumn> {

	public WritableDoubleValue(final WritableDoubleColumn column) {
		super(column);
	}

	public void setDoubleValue(final double value) {
		m_column.setDoubleValue(value);
	}
}
