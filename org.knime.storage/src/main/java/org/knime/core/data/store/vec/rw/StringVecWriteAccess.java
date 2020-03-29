package org.knime.core.data.store.vec.rw;

public interface StringVecWriteAccess extends VecWriteAccess, StringVecAccess {
	void set(String val);
}
