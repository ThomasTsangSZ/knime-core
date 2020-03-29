package org.knime.core.data.store.chunk;

import org.knime.core.data.store.MutableValue;
import org.knime.core.data.store.vec.VecReadAccessible;

// Chunk of data. 
// accessible for vecs (read/write)
public interface Chunk extends AutoCloseable, VecReadAccessible<MutableValue> {
}
