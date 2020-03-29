package org.knime.core.data.store.vec;

import org.knime.core.data.store.Value;

public interface VecAccessible<V extends Value, A extends VecAccess<V>> {

	A access();

	VecSchema schema();
}
