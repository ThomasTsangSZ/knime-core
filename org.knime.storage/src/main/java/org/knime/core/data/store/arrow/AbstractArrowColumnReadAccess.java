package org.knime.core.data.store.arrow;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.BatchColumnReadAccess;

public abstract class AbstractArrowColumnReadAccess<V extends FieldVector> extends AbstractArrowColumnAccess<V>
		implements BatchColumnReadAccess {

	protected AbstractArrowColumnReadAccess(final V vector) {
		super(vector);
	}

	@Override
	public boolean isMissing() {
		return m_vector.isNull(m_idx);
	}

	@Override
	public boolean canForward() {
		return m_idx + 1 < m_vector.getValueCount();
	}

}
