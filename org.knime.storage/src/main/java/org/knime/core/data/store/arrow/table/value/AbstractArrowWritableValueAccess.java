
package org.knime.core.data.store.arrow.table.value;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.table.value.WritableValueAccess;

public abstract class AbstractArrowWritableValueAccess<V extends ValueVector> implements WritableValueAccess {

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
	public void setMissing() {
		// TODO: Is this actually correct (especially when reusing the vector)? Or
		// use setNull instead? knime-python does it like here.
		m_vector.setValueCount(m_index + 1);
	}

}
