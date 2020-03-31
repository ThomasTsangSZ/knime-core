package org.knime.core.data.store.arrow.table;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.partition.ColumnPartition;

// simple wrapper around a value vector
public class ArrowColumnPartition<F extends ValueVector> implements ColumnPartition {

	private F m_vector;

	public ArrowColumnPartition(F vector) {
		m_vector = vector;
	}

	@Override
	public void close() throws Exception {
		m_vector.close();
	}

	@Override
	public int getValueCount() {
		return m_vector.getValueCount();
	}

	@Override
	public int getValueCapacity() {
		return m_vector.getValueCapacity();
	}

	public F get() {
		return m_vector;
	}

}
