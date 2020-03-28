package org.knime.core.data.store;

public interface BatchColumnWriteAccess extends BatchColumnAccess {
	// set value in column at current pos missing
	void setMissing();
}
