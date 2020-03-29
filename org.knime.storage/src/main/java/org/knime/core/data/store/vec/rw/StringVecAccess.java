package org.knime.core.data.store.vec.rw;

import org.knime.core.data.store.vec.VecType;

public interface StringVecAccess extends VecReadAccess {
	@Override
	default VecType getType() {
		return VecType.STRING;
	}
}
