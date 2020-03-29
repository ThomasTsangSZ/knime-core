
package org.knime.core.data.store.vec.arrow;

import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.vec.rw.BooleanVecWriteAccess;

final class ArrowBooleanVecWriteAccess //
	extends AbstractArrowVecWriteAccess<BitVector> //
	implements BooleanVecWriteAccess
{

	public ArrowBooleanVecWriteAccess(final BitVector vector) {
		super(vector);
	}

	@Override
	public void setBooleanValue(final boolean value) {
		m_vector.set(m_idx, value ? 1 : 0);
		m_vector.setValueCount(m_idx + 1);
	}
}
