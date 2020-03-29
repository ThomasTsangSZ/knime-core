
package org.knime.core.data.store.table.row.impl;

import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.table.row.WritableDataValue;

public abstract class AbstractWritableDataValue<C extends WritableColumn> implements WritableDataValue {

	protected final C m_column;

	public AbstractWritableDataValue(final C column) {
		m_column = column;
	}

	@Override
	public void setMissing() {
		m_column.setMissing();
	}
}
