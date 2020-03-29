package org.knime.core.data.store.arrow;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.vec.rw.VecWriteAccess;

public abstract class AbstractArrowVecWriteAccess<V extends FieldVector> extends AbstractArrowVecAccess<V>
		implements VecWriteAccess {

	protected AbstractArrowVecWriteAccess(V vector) {
		super(vector);
	}

	@Override
	public boolean canForward() {
		return m_idx + 1 < m_vector.getValueCapacity();
	}
}
