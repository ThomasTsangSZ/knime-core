
package org.knime.core.data.vector.table;

import org.knime.core.data.table.value.ReadableValue;
import org.knime.core.data.table.value.WritableValue;
import org.knime.core.data.vector.Vector;

// TODO split into Readable / Writable
public interface VectorValue<T> extends ReadableValue, WritableValue {

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
	void updatePartition(Vector<T> partition);
}
