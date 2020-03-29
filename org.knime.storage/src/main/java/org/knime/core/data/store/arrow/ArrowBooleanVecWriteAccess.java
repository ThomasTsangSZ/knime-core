package org.knime.core.data.store.arrow;

import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.vec.rw.BooleanVecWriteAccess;

public class ArrowBooleanVecWriteAccess extends AbstractArrowVecWriteAccess<BitVector>
		implements BooleanVecWriteAccess {

	protected ArrowBooleanVecWriteAccess(BitVector vector) {
		super(vector);
	}

	@Override
	public void setMissing() {
		m_vector.setNull(m_idx);
	}

	@Override
	public void set(boolean val) {
		// TODO double check if this is correct
		m_vector.set(m_idx, val ? 1 : 0);
	}

}
