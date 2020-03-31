
package org.knime.core.data.store.arrow.table.value;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.table.value.ReadableValueAccess;

public abstract class AbstractArrowReadableValueAccess<V extends ValueVector> implements ReadableValueAccess {

	protected int m_index = -1;

	protected V m_vector;

	public void incIndex() {
		m_index++;
	}

	public final V getVector() {
		return m_vector;
	}

	public void setVector(final V vector) {
		m_vector = vector;
	}

	@Override
	public boolean isMissing() {
		return m_vector.isNull(m_index);
	}
}
