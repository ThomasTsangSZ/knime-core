
package org.knime.core.data.store.table.column.impl;

import org.knime.core.data.store.vec.rw.DoubleVecReadAccess;

public final class DefaultReadableDoubleColumn implements ReadableDoubleColumn {

	private final DoubleVecReadAccess m_access;

	public DefaultReadableDoubleColumn(final DoubleVecReadAccess access) {
		m_access = access;
	}

	@Override
	public boolean canFwd() {
		return m_access.canFwd();
	}

	@Override
	public void fwd() {
		m_access.fwd();
	}

	@Override
	public boolean isMissing() {
		return m_access.isMissing();
	}

	@Override
	public double getDoubleValue() {
		return m_access.getDoubleValue();
	}

	@Override
	public void close() throws Exception {
		m_access.close();
	}
}
