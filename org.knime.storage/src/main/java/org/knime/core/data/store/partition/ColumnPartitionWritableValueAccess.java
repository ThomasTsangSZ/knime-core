
package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.value.WritableValueAccess;

public interface ColumnPartitionWritableValueAccess<T> extends WritableValueAccess {

	void incIndex();

	void updatePartition(T partition);
}
