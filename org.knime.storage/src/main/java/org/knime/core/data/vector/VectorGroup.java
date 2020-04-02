
package org.knime.core.data.vector;

import org.knime.core.data.vector.table.VectorValue;

// TODO would it help us to make use of the sequential character of the data?
public interface VectorGroup<K, D> {

	/**
	 * @return a <O> managed by the store. Either a new object or an existing one.
	 */
	Vector<D> getOrCreate(K key);

	/**
	 * @return number of managed objects of this store. Including all childStores.
	 */
	long numVectors();

	/**
	 * @return created a value to access vectors of this vector group
	 */
	VectorValue<D> createLinkedValue();
}
