package org.knime.core.data.arrow;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.cache.RefCountingPartition;

public class ArrowPartition<V extends FieldVector> implements RefCountingPartition<V> {

	private final V m_vector;
	private long m_capacity;
	private final long m_index;
	private final AtomicInteger m_refCount;

	// Read case
	public ArrowPartition(V vector, long index) {
		this(vector, -1, index);
	}

	// write case
	public ArrowPartition(V vector, final long capacity, final long index) {
		m_capacity = capacity;
		m_vector = vector;
		m_capacity = capacity;
		m_index = index;

		// TODO thread safety? Introduce central ref manager?
		m_refCount = new AtomicInteger(1);
	}

	@Override
	public void close() throws Exception {
		if (m_refCount.decrementAndGet() == 0)
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

	@Override
	public long getIndex() {
		return m_index;
	}

	@Override
	public void incRefCount() {
		m_refCount.incrementAndGet();
	}
}
