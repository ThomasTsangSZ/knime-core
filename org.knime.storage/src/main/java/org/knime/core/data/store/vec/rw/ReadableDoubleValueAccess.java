package org.knime.core.data.store.vec.rw;

import org.knime.core.data.store.table.row.ReadableValueAccess;

public interface ReadableDoubleValueAccess extends ReadableValueAccess {

	double getDoubleValue();

}
