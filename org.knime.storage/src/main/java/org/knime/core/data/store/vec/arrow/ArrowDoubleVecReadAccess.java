
package org.knime.core.data.store.vec.arrow;

import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.vec.rw.ReadableDoubleVectorAccess;

final class ArrowDoubleVecReadAccess extends AbstractArrowVecReadAccess<Float8Vector> implements ReadableDoubleVectorAccess {

	public ArrowDoubleVecReadAccess(final Float8Vector vector) {
		super(vector);
	}

	@Override
	public double getDoubleValue() {
		return m_vector.get(m_idx);
	}
}
