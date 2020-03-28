package org.knime.core.data.store;

import org.knime.core.data.store.vec.VecReadAccess;
import org.knime.core.data.store.vec.VecWriteAccess;

public interface Chunk extends AutoCloseable {

	ChunkSchema getSchema();

	VecReadAccess getReadAccessAt(int idx);
	
	VecWriteAccess getWriteAccessAt(int idx);

}
