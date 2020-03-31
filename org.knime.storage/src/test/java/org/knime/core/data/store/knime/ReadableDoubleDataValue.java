
package org.knime.core.data.store.knime;

import org.knime.core.data.store.table.value.ReadableDoubleValueAccess;

public final class ReadableDoubleDataValue extends AbstractReadableDataValue<ReadableDoubleValueAccess> {

	public ReadableDoubleDataValue(final ReadableDoubleValueAccess valueAccess) {
		super(valueAccess);
	}

	public double getDoubleValue() {
		return m_value.getDoubleValue();
	}
}
