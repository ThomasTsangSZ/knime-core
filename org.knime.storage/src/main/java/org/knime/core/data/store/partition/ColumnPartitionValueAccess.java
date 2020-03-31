
package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.value.ReadableValueAccess;
import org.knime.core.data.store.table.value.WritableValueAccess;

public interface ColumnPartitionValueAccess<T> extends ReadableValueAccess, WritableValueAccess {

	void incIndex();

	void updatePartition(ColumnPartition<T> partition);
}
