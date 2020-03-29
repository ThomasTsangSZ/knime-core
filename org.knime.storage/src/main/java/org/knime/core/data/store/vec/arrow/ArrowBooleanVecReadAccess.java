
package org.knime.core.data.store.vec.arrow;

import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.vec.rw.BooleanVecReadAccess;

final class ArrowBooleanVecReadAccess extends AbstractArrowVecReadAccess<BitVector> implements BooleanVecReadAccess {

	public ArrowBooleanVecReadAccess(final BitVector vector) {
		super(vector);
	}

	@Override
	public boolean getBooleanValue() {
		return m_vector.get(m_idx) > 0;
	}
}
