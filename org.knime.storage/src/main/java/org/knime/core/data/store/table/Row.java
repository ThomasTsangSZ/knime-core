package org.knime.core.data.store.table;

import org.knime.core.data.store.DataValue;

public interface Row<V extends DataValue> {
	long numValues();

	V valueAt(int idx);
}
