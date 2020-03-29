package org.knime.core.data.store.vec.rw;

public interface BooleanVecWriteAccess extends BooleanVecAccess, VecWriteAccess {
	void set(boolean val);
}
