
package org.knime.core.data.store.value;

import org.knime.core.data.store.MutableDataValue;
import org.knime.core.data.store.vec.VecType;

public class MutableDoubleValue implements /* DoubleValue, */ MutableDataValue {

	@Override
	public VecType type() {
		return VecType.DOUBLE;
	}

	public double getDoubleValue() {
		// TODO: get double from vector
		throw new IllegalStateException("nyi");
	}

	public void setDoubleValue(final double value) {
		// TODO: set double in vector
		throw new IllegalStateException("nyi");
	}
}
