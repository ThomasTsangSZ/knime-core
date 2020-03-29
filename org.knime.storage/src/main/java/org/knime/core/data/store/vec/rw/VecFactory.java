package org.knime.core.data.store.vec.rw;

import org.knime.core.data.store.vec.VecAccessible;
import org.knime.core.data.store.vec.VecType;

public interface VecFactory {
	VecAccessible create(VecType type);
}
