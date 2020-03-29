package org.knime.core.data.store;

import org.knime.core.data.store.vec.rw.VecReadAccess;
import org.knime.core.data.store.vec.rw.VecWriteAccess;

public interface Chunk extends AutoCloseable {

	VecGroupSchema getSchema();

	VecReadAccess getReadAccessAt(int idx);
	
	VecWriteAccess getWriteAccessAt(int idx);

}
