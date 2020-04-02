package org.knime.core.data.vector;

import java.io.Flushable;

import org.knime.core.data.table.column.NativeType;

/*
 * Properties of a DataStore:
 *  -> If not closed, data is held available to consumers
 *  -> If flushed, data is persisted on disc (TODO introduce FlushableDataStore to distinguish an in-memory store from a flushable store)
 *  -> If closed, all memory is released. Potentially written data is kept.
 *  
 *  -> Important: When on 'close' or 'destroy' memory is only released for objects with no external references. 
 */
public interface VectorStore<K> extends AutoCloseable, Flushable {

	<O> VectorGroup<K, O> createGroup(final NativeType... type);

	/**
	 * Destroys the store entirely. External references will keep their references
	 * to created objects.
	 * 
	 * @throws Exception
	 */
	void destroy() throws Exception;
}
