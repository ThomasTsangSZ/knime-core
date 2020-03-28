package org.knime.core.data.store;

public interface BatchColumnAccess {
	BatchColumnType getType();

	void fwd();

	boolean canForward();
}
