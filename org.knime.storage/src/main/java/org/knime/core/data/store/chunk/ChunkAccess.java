
package org.knime.core.data.store.chunk;

public interface ChunkAccess extends AutoCloseable {

	boolean hasNext();

	void fwd();

	VecAccessible get();

	default VecAccessible next() {
		fwd();
		return get();
	}
}
