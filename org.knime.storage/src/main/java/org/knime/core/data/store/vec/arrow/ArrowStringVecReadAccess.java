package org.knime.core.data.store.vec.arrow;

import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.store.vec.rw.StringVecReadAccess;

final class ArrowStringVecReadAccess extends AbstractArrowVecReadAccess<VarCharVector> implements StringVecReadAccess {

	protected ArrowStringVecReadAccess(VarCharVector vector) {
		super(vector);
	}

	@Override
	public String get() {
		// TODO is there a more efficient way?
		return m_vector.getObject(m_idx).toString();
	}
}