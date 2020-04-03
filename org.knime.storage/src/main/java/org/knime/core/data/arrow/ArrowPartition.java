package org.knime.core.data.arrow;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.table.column.Partition;

public class ArrowPartition<V extends FieldVector> implements Partition<V> {

	private V m_vector;
	private long m_capacity;

	// Read case
	public ArrowPartition(V vector) {
		m_vector = vector;
		m_capacity = -1;
	}

	public ArrowPartition(V vector, final long capacity) {
		m_vector = vector;
		m_capacity = capacity;
	}

	@Override
	public void close() throws Exception {
		m_vector.close();
	}

	@Override
	public long getCapacity() {
		return m_capacity;
	}

	@Override
	public void setNumValues(int numValues) {
		m_vector.setValueCount(numValues);
	}

	@Override
	public int getNumValues() {
		// calling numValues on vector is super slow
		return m_vector.getValueCount();
	}

	@Override
	public V get() {
		return m_vector;
	}
}
