
package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.value.ReadableValueAccess;

public interface ColumnPartitionReadableValueAccess<T> extends ReadableValueAccess {

	void incIndex();

	void updatePartition(T partition);
}
