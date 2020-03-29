package org.knime.core.data.store.vec;

public interface VecSchema {

	int getNumVecs();

	VecType getVecTypeAt(int i);

}
