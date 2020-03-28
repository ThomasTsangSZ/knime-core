package org.knime.core.data.store.arrow;

import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.BooleanColumnReadAccess;

final class ArrowBooleanColumnReadAccess extends AbstractArrowColumnReadAccess<BitVector>
		implements BooleanColumnReadAccess {

	protected ArrowBooleanColumnReadAccess(BitVector vector) {
		super(vector);
	}

	@Override
	public boolean get() {
		return m_vector.getObject(m_idx);
	}
}