package org.knime.core.data.store.vec.rw;

import org.knime.core.data.store.vec.VecType;

public interface BooleanVecAccess extends VecReadAccess {
	@Override
	default VecType getType() {
		return VecType.BOOLEAN;
	}
}
