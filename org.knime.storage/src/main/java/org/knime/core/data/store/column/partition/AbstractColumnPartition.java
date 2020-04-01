package org.knime.core.data.store.column.partition;

public abstract class AbstractColumnPartition<V> implements ColumnPartition<V> {

	protected final V m_storage;
	private final int m_maxSize;
	private int m_numValues;

	public AbstractColumnPartition(final V storage, final int maxSize, final int numValues) {
		m_storage = storage;
		m_maxSize = maxSize;
		m_numValues = numValues;
	}

	public AbstractColumnPartition(final V storage, final int maxSize) {
		m_storage = storage;
		m_maxSize = maxSize;
	}

	@Override
	public V get() {
		return m_storage;
	}

	@Override
	public int getCapacity() {
		return m_maxSize;
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