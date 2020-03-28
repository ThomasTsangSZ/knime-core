package org.knime.core.data.store;

import org.knime.core.data.store.vec.VecType;

public interface ChunkSchema {

	int getNumVecs();

	VecType getVecTypeAt(int i);

}
