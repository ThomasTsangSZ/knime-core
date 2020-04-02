package org.knime.core.data.table.column;

public interface PartitionedColumn<D> {

	/**
	 * @return created a value to access vectors of this vector group
	 */
	PartitionValue<D> createLinkedValue();
}
