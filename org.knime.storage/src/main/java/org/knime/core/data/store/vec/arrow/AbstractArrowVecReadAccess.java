
package org.knime.core.data.store.vec.arrow;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.vec.rw.ReadableVectorAccess;

public abstract class AbstractArrowVecReadAccess<V extends FieldVector> //
	extends AbstractArrowVecAccess<V> //
	implements ReadableVectorAccess
{

	public AbstractArrowVecReadAccess(final V vector) {
		super(vector);
	}

	@Override
	public boolean canFwd() {
		return m_idx + 1 < m_vector.getValueCount();
	}

	@Override
	public boolean isMissing() {
		return m_vector.isNull(m_idx);
	}
}
