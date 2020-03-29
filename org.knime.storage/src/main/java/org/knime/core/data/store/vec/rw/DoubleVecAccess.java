package org.knime.core.data.store.vec.rw;

import org.knime.core.data.store.vec.VecType;

public interface DoubleVecAccess extends VecReadAccess {
	@Override
	default VecType getType() {
		return VecType.DOUBLE;
	}
}
