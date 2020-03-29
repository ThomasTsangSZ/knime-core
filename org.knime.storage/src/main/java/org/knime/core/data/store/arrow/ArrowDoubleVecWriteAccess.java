package org.knime.core.data.store.arrow;

import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.vec.rw.DoubleVecWriteAccess;

public class ArrowDoubleVecWriteAccess extends AbstractArrowVecWriteAccess<Float8Vector>
		implements DoubleVecWriteAccess {

	protected ArrowDoubleVecWriteAccess(Float8Vector vector) {
		super(vector);
	}

	@Override
	public void setMissing() {
		m_vector.setNull(m_idx);
	}

	@Override
	public void set(double val) {
		m_vector.set(m_idx, val);
	}
}
