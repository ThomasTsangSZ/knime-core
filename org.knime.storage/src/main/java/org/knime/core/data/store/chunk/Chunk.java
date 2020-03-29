package org.knime.core.data.store.chunk;

import org.knime.core.data.store.vec.VecAccessible;

// Chunk of data. 
// accessible for vecs (read/write)
public interface Chunk extends AutoCloseable, VecAccessible {
}
