package org.knime.core.data.store.knime;

import org.knime.core.data.store.vec.rw.WritableDoubleValueAccess;

public final class WritableDoubleDataValue extends AbstractWritableDataValue<WritableDoubleValueAccess> {

	public WritableDoubleDataValue(final WritableDoubleValueAccess valueAccess) {
		super(valueAccess);
	}

	public void setDoubleValue(final double value) {
		m_valueAccess.setDoubleValue(value);
	}
}
