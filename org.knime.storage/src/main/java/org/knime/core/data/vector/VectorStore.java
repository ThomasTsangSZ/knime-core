package org.knime.core.data.vector;

import org.knime.core.data.vector.table.VectorValue;

public interface VectorStore<D> {

	/**
	 * @return created a value to access vectors of this vector group
	 */
	VectorValue<D> createLinkedValue();
}
