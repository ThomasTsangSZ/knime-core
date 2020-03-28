package org.knime.core.data.store.vec;

public interface BooleanVecAccess extends VecAccess {
	@Override
	default VecType getType() {
		return VecType.BOOLEAN;
	}
}
