package org.knime.core.data.store.table;

import org.knime.core.data.store.Value;

public interface Row<V extends Value> {
	long numValues();

	V valueAt(int idx);
}
