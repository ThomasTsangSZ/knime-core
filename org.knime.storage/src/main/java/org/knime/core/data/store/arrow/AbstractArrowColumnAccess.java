package org.knime.core.data.store.arrow;

import org.knime.core.data.store.BatchColumnAccess;

public abstract class AbstractArrowColumnAccess<V> implements BatchColumnAccess {

	protected V m_vector;
	protected int m_idx = -1;

	protected AbstractArrowColumnAccess(final V vector) {
		m_vector = vector;
	}

	@Override
	public void fwd() {
		m_idx++;
	}
}
