package org.knime.core.data.store.vec;

import org.knime.core.data.store.Access;
import org.knime.core.data.store.MutableDataValue;

public interface VecAccess extends Access<MutableDataValue[]> {
	// NB: Marker interface to access vec values.
}
