
package org.knime.core.data.partition;

import org.knime.core.data.table.value.ReadableValue;
import org.knime.core.data.table.value.WritableValue;

// TODO split into Readable / Writable
public interface PartitionValue<T> extends ReadableValue, WritableValue {

	/**
	 * Increments the internal index by one.
	 */
	void incIndex();

	/**
	 * Resets the internal index such that the next call to {@link #incIndex()} sets
	 * it to the first element of the given partition.
	 *
	 * @param partition The partition to be accessed by this access.
	 */
	void updatePartition(Partition<T> partition);
}
