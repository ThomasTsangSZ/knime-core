package org.knime.core.data.store.vec.rw;

public interface VecWriteAccess extends VecAccess {
	// set value in column at current pos missing
	void setMissing();
}
