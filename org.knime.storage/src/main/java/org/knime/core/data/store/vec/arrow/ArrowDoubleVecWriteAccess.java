
package org.knime.core.data.store.vec.arrow;

import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.vec.rw.DoubleVecWriteAccess;

final class ArrowDoubleVecWriteAccess//
	extends AbstractArrowVecWriteAccess<Float8Vector> //
	implements DoubleVecWriteAccess
{

	public ArrowDoubleVecWriteAccess(final Float8Vector vector) {
		super(vector);
	}

	@Override
	public void setDoubleValue(final double val) {
		m_vector.set(m_idx, val);
		m_vector.setValueCount(m_idx + 1);
	}
}
