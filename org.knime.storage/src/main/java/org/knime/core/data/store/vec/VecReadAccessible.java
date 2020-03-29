package org.knime.core.data.store.vec;

public interface VecReadAccessible {
	VecReadAccess access();

	VecSchema schema();
}
