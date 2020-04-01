
package org.knime.core.data.store.column.partition;

public abstract class AbstractColumnPartition<V> implements ColumnPartition<V> {

	protected final V m_storage;

	private final long m_capacity;

	private long m_numValues;

	public AbstractColumnPartition(final V storage, final long capacity, final long numValues) {
		this(storage, capacity);
		m_numValues = numValues;
	}

	public AbstractColumnPartition(final V storage, final long capacity) {
		m_storage = storage;
		m_capacity = capacity;
	}

	@Override
	public V getStorage() {
		return m_storage;
	}

	@Override
	public long getCapacity() {
		return m_capacity;
	}

	@Override
	public long getNumValues() {
		return m_numValues;
	}

	@Override
	public void setNumValues(final long numValues) {
		m_numValues = numValues;
	}
}
