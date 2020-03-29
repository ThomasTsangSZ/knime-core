package org.knime.core.data.store.vec;

public interface VecAccessible {

	VecAccess access();

	VecSchema schema();
}
