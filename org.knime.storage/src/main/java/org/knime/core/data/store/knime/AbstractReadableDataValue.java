
package org.knime.core.data.store.knime;

import org.knime.core.data.store.knime.StorageTest.DataValue;
import org.knime.core.data.store.table.row.ReadableValueAccess;

public abstract class AbstractReadableDataValue<V extends ReadableValueAccess> implements DataValue {

	protected final V m_value;

	public AbstractReadableDataValue(final V vector) {
		m_value = vector;
	}
}
