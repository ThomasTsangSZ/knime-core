
package org.knime.core.data.store.knime;

import org.knime.core.data.store.knime.StorageTest.WritableDataValue;
import org.knime.core.data.store.table.value.WritableValueAccess;

public abstract class AbstractWritableDataValue<V extends WritableValueAccess> implements WritableDataValue {

	protected final V m_valueAccess;

	public AbstractWritableDataValue(final V valueAccess) {
		m_valueAccess = valueAccess;
	}
}
