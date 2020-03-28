package org.knime.core.data.store.vec;

public interface DoubleVecAccess extends VecAccess {
	@Override
	default VecType getType() {
		return VecType.DOUBLE;
	}
}
