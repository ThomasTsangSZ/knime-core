package org.knime.core.data.store.vec;

public interface StringVecAccess extends VecAccess {
	@Override
	default VecType getType() {
		return VecType.STRING;
	}
}
