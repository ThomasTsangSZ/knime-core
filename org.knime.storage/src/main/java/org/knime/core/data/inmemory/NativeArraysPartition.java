package org.knime.core.data.inmemory;

import java.util.concurrent.atomic.AtomicInteger;

import org.knime.core.data.cache.RefCountingPartition;
import org.knime.core.data.inmemory.array.NativeArray;

public class NativeArraysPartition<V extends NativeArray<?>> implements RefCountingPartition<V> {

	private final V m_array;
	private long m_capacity;
	private final long m_index;
	private final AtomicInteger m_refCount;
	private int m_numValues;

	// Read case
	public NativeArraysPartition(V vector, long index) {
		this(vector, -1, index);
	}

	// write case
	public NativeArraysPartition(V vector, final long capacity, final long index) {
		m_capacity = capacity;
		m_array = vector;
		m_capacity = capacity;
		m_index = index;

		// TODO thread safety? Introduce central ref manager?
		m_refCount = new AtomicInteger(1);
	}

	@Override
	public void close() throws Exception {
		if (m_refCount.decrementAndGet() == 0)
			m_array.close();
	}

	@Override
	public long getCapacity() {
		return m_capacity;
	}

	@Override
	public void setNumValuesWritten(int numValues) {
		m_numValues = numValues;
	}

	@Override
	public int getNumValuesWritten() {
		return m_numValues;
	}

	@Override
	public V get() {
		return m_array;
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
