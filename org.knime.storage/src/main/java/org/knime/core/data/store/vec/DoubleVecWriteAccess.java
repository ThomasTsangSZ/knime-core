package org.knime.core.data.store.vec;

public interface DoubleVecWriteAccess extends DoubleVecAccess, VecWriteAccess {
	void set(double val);
}
