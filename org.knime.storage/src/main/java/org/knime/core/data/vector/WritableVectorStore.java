
package org.knime.core.data.vector;

import org.apache.commons.lang3.tuple.Pair;
import org.knime.core.data.vector.table.VectorValue;

// TODO would it help us to make use of the sequential character of the data?
// TODO alternative name: partitioned vector
public interface WritableVectorStore<K, D> extends VectorStore<D> {

	/**
	 * @return a <Pair<K, Vector<D>> managed by the store.
	 */
	Pair<K, Vector<D>> add();

	/**
	 * @return number of managed objects of this store. Including all childStores.
	 */
	long numVectors();

	/**
	 * @return created a value to access vectors of this vector group
	 */
	VectorValue<D> createLinkedValue();
}
