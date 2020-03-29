package org.knime.core.data.store.vec.rw;

public interface DoubleVecWriteAccess extends DoubleVecAccess, VecWriteAccess {
	void set(double val);
}
