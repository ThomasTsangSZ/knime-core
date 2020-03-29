package org.knime.core.data.store.table;

import org.knime.core.data.store.DataValue;

public interface TableAccessible<V extends DataValue> {
	TableAccess<V> access();
}
