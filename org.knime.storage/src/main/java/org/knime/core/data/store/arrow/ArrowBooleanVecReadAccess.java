package org.knime.core.data.store.arrow;

import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.vec.rw.BooleanVecReadAccess;

final class ArrowBooleanVecReadAccess extends AbstractArrowVecReadAccess<BitVector> implements BooleanVecReadAccess {

	protected ArrowBooleanVecReadAccess(BitVector vector) {
		super(vector);
	}

	@Override
	public boolean get() {
		// TODO check if correct
		return m_vector.get(m_idx) == 1;
	}
}