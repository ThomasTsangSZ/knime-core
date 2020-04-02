
package org.knime.core.data.arrow.vector;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.vector.Vector;

public final class ArrowVector<T extends FieldVector> implements Vector<T> {

	private T m_storage;

	public ArrowVector(final T storage) {
		m_storage = storage;
	}

	@Override
	public void close() throws Exception {
		m_storage.close();
	}

	@Override
	public T get() {
		return m_storage;
	}

	@Override
	public long getCapacity() {
		return m_storage.getValueCapacity();
	}

	@Override
	public void setNumValues(int numValues) {
		m_storage.setValueCount(numValues);
	}

	@Override
	public int getNumValues() {
		return m_storage.getValueCount();
	}
}
