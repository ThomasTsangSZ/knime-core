package org.knime.core.data.inmemory.array;

public class AbstractNativeArray<A> implements NativeArray<A> {

	private long[] m_isMissing;
	private int m_capacity;

	protected A m_array;

	AbstractNativeArray(A array, int capacity) {
		m_capacity = capacity;
		m_array = array;
		m_isMissing = new long[((int) capacity / 64) + 1];
	}

	@Override
	public boolean isMissing(int index) {
		// NB: inspired by imglib2
		return 1 == ((m_isMissing[(index >>> 6)] >>> ((index & 63))) & 1l);
	}

	@Override
	public void setMissing(int index) {
		// NB: inspired by imglib
		final int i1 = index >>> 6;
		m_isMissing[i1] = m_isMissing[i1] | 1l << (index & 63);
	}

	public A get() {
		return m_array;
	}

	@Override
	public void close() throws Exception {
		m_array = null;
		m_isMissing = null;
	}

	@Override
	public long size() {
		return m_capacity;
	}

}
