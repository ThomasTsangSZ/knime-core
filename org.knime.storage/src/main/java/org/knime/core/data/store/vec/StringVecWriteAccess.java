package org.knime.core.data.store.vec;

public interface StringVecWriteAccess extends VecWriteAccess, StringVecAccess {
	void set(String val);
}
