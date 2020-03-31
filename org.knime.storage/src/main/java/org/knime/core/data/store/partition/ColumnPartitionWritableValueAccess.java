package org.knime.core.data.store.partition;

import org.knime.core.data.store.table.value.WritableValueAccess;

public interface ColumnPartitionWritableValueAccess extends WritableValueAccess, ColumnPartitionValueAccess {
	// NB: Marker interface
}