package org.knime.core.data.store.column.partition;

public class DefaultColumnPartition<T extends AutoCloseable> implements ColumnPartition<T> {

	private final T m_storage;
	private final long m_index;
	private final int m_maxSize;
	private int m_numValues;

	public DefaultColumnPartition(final T vector, final long partitionIdx, final int maxSize) {
		m_storage = vector;
		m_index = partitionIdx;
		m_maxSize = maxSize;
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
	public int getCapacity() {
		return m_maxSize;
	}

	@Override
	public long getIndex() {
		return m_index;
	}

	@Override
	public int getNumValues() {
		return m_numValues;
	}

	@Override
	public void setNumValues(int numValues) {
		m_numValues = numValues;
	}

}