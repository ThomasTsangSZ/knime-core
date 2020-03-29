
package org.knime.core.data.store.table.row.impl;

import org.knime.core.data.store.table.column.ReadableColumn;
import org.knime.core.data.store.table.row.ReadableDataValue;

public abstract class AbstractReadableDataValue<C extends ReadableColumn> implements ReadableDataValue {

	protected final C m_column;

	public AbstractReadableDataValue(final C column) {
		m_column = column;
	}

	@Override
	public boolean isMissing() {
		return m_column.isMissing();
	}
}
