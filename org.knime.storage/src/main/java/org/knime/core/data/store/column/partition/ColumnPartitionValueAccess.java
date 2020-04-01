
package org.knime.core.data.store.column.partition;

import org.knime.core.data.store.column.value.ReadableValueAccess;
import org.knime.core.data.store.column.value.WritableValueAccess;

public interface ColumnPartitionValueAccess<T> extends ReadableValueAccess, WritableValueAccess {

	/**
	 * Increments internal index by one
	 */
	void incIndex();

	/**
	 * Will reset internal index to -1;
	 * 
	 * @param partition the partition to be accessed by this ValueAccess
	 */
	void updatePartition(ColumnPartition<T> partition);
}
