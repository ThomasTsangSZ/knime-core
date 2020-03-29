package org.knime.core.data.store.vec.rw;

import org.knime.core.data.store.vec.VecType;

public interface StringVecAccess extends VecAccess {
	@Override
	default VecType getType() {
		return VecType.STRING;
	}
}
