package org.knime.core.data.store.vec.rw;

public interface VecWriteAccess extends VecReadAccess {
	// set value in column at current pos missing
	void setMissing();
}
