
package org.knime.core.data.store.table.row.impl;

import org.knime.core.data.store.table.column.impl.ReadableDoubleColumn;

public final class ReadableDoubleValue extends AbstractReadableDataValue<ReadableDoubleColumn> {

	public ReadableDoubleValue(final ReadableDoubleColumn column) {
		super(column);
	}

	public double getDoubleValue() {
		return m_column.getDoubleValue();
	}
}
