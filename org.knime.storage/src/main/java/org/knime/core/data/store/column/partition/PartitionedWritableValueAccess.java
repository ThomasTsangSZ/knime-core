
package org.knime.core.data.store.column.partition;

import org.knime.core.data.store.column.value.WritableValueAccess;

public interface PartitionedWritableValueAccess<T> extends WritableValueAccess {

	/**
	 * Increments the internal index by one.
	 */
	void incIndex();

	/**
	 * Resets the internal index such that the next call to {@link #incIndex()}
	 * sets it to the first element of the given partition.
	 *
	 * @param partition The partition to be accessed by this access.
	 */
	void updatePartition(ColumnPartition<T> partition);
}
