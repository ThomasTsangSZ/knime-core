
package org.knime.core.data.store.vec.arrow;

import org.apache.arrow.vector.FieldVector;

public abstract class AbstractArrowVecAccess<V extends FieldVector> implements AutoCloseable {

	protected final V m_vector;

	protected int m_idx = -1;

	public AbstractArrowVecAccess(final V vector) {
		m_vector = vector;
	}

	public void fwd() {
		m_idx++;
	}

	@Override
	public void close() throws Exception {
		m_vector.close();
	}
}
