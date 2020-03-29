package org.knime.core.data.store;

import org.knime.core.data.store.vec.VecType;

// Proxy to access values
public interface Value {
	VecType type();
}
