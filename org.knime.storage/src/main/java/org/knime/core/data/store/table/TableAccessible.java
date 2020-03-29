package org.knime.core.data.store.table;

import org.knime.core.data.store.Value;

public interface TableAccessible<V extends Value> {
	TableAccess<V> access();
}
