
package org.knime.core.data.store.chunk;

import org.knime.core.data.store.VecAccessible;

public interface ChunkAccess extends AutoCloseable {

	boolean hasNext();

	void fwd();

	VecAccessible get();

	default VecAccessible next() {
		fwd();
		return get();
	}
}
