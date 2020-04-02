package org.knime.core.data.vector;

import org.knime.core.data.table.column.NativeType;

/*
 * Properties of a DataStore:
 *  -> If not closed, data is held available to consumers
 *  -> If flushed, data is persisted on disc
 *  -> If closed, all memory is released. Potentially written data is kept
 *  
 *  -> Important: When on 'close' or 'destroy' memory is only released for objects with no external references. 
 */

// TODO read vs. write in groups? use-case KNIME read-only table.
public interface VectorStoreRoot<K> extends RefManaged {

	/**
	 * @param <O>
	 * @param type
	 * @return a new vector group. Reference counter increased before return.
	 */
	ReadableVectorStore<K, ?> createReadableStore(final NativeType... type);
	
	/**
	 * @param <O>
	 * @param type
	 * @return a new vector group. Reference counter increased before return.
	 */
	WritableVectorStore<K, ?> createWritableStore(final NativeType... type);

}
