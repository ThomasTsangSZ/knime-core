
package org.knime.core.data.store.knime;

import org.knime.core.data.store.vec.rw.ReadableDoubleValueAccess;

public final class ReadableDoubleDataValue extends AbstractReadableDataValue<ReadableDoubleValueAccess> {

	public ReadableDoubleDataValue(final ReadableDoubleValueAccess valueAccess) {
		super(valueAccess);
	}

	public double getDoubleValue() {
		return m_value.getDoubleValue();
	}
}
