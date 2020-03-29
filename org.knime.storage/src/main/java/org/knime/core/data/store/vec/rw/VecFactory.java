package org.knime.core.data.store.vec.rw;

import org.knime.core.data.store.vec.VecReadAccessible;
import org.knime.core.data.store.vec.VecType;

public interface VecFactory {
	VecReadAccessible create(VecType type);
}
