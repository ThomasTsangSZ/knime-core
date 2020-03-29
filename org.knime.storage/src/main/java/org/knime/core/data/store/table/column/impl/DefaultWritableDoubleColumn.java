
package org.knime.core.data.store.table.column.impl;

import org.knime.core.data.store.vec.rw.DoubleVecWriteAccess;

public final class DefaultWritableDoubleColumn implements WritableDoubleColumn {

	private final DoubleVecWriteAccess m_access;

	public DefaultWritableDoubleColumn(final DoubleVecWriteAccess access) {
		m_access = access;
	}

	@Override
	public void fwd() {
		m_access.fwd();
	}

	@Override
	public void setMissing() {
		m_access.setMissing();
	}

	@Override
	public void setDoubleValue(final double value) {
		m_access.setDoubleValue(value);
	}

	@Override
	public void close() throws Exception {
		m_access.close();
	}
}
