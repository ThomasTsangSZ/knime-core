package org.knime.core.data.vector;

import org.knime.core.data.vector.table.VectorValue;

public interface VectorStore<D> extends RefManaged {

	/**
	 * @return number of managed objects of this store. Including all childStores.
	 */
	long numVectors();

	/**
	 * @return created a value to access vectors of this vector group
	 */
	VectorValue<D> createLinkedValue();
}
