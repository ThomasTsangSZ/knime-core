package org.knime.core.data.store.table;

import org.knime.core.data.store.Value;

public interface Row {
	Value valueAt(int i);

	long numValues();
}
