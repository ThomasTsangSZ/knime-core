package org.knime.core.data.store.vec.arrow;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.vec.VecReadAccess;

public abstract class AbstractArrowVecAccess<V extends FieldVector> implements VecReadAccess {

	protected V m_vector;
	protected int m_idx = -1;

	protected AbstractArrowVecAccess(final V vector) {
		m_vector = vector;
	}

	@Override
	public void fwd() {
		m_idx++;
	}
}
