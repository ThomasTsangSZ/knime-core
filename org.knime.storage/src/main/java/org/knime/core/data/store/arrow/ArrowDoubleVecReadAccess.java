package org.knime.core.data.store.arrow;

import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.vec.rw.DoubleVecReadAccess;

final class ArrowDoubleVecReadAccess extends AbstractArrowVecReadAccess<Float8Vector> implements DoubleVecReadAccess {

	protected ArrowDoubleVecReadAccess(Float8Vector vector) {
		super(vector);
	}

	@Override
	public double get() {
		return m_vector.get(m_idx);
	}
}