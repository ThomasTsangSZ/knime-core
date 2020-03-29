package org.knime.core.data.store.vec;

import org.knime.core.data.store.Access;
import org.knime.core.data.store.MutableValue;

public interface VecAccess extends Access<MutableValue[]> {
	// NB: Marker interface to access vec values.
}
