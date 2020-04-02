package org.knime.core.data.vector;

import java.io.Flushable;

// VectorStore which can be flushed. Non-flushable = in-memory only
public interface FlushableVectorStore<K> extends VectorStoreRoot<K>, Flushable {
// NB: Marker interface
}
