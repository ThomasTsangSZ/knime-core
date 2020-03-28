package org.knime.core.data.store.vec;

public interface VecAccess {
	VecType getType();

	void fwd();

	boolean canForward();
}
