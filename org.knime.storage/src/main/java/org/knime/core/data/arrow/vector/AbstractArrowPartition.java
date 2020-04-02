
package org.knime.core.data.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.table.column.Partition;

// TODO composition over inheritance? :-(
abstract class AbstractArrowPartition<V extends FieldVector> implements Partition<V> {

	private final BufferAllocator m_allocator;

	private final int m_partitionCapacity;

	private V m_vector;

	public AbstractArrowPartition(final BufferAllocator allocator, final int partitionCapacity) {
		m_allocator = allocator;
		m_partitionCapacity = partitionCapacity;
		m_vector = create(m_allocator, m_partitionCapacity);
	}

	@Override
	public V get() {
		return m_vector;
	}

	abstract V create(BufferAllocator allocator, int capacity);

	@Override
	public void close() throws Exception {
		m_vector.close();
	}

	@Override
	public long getCapacity() {
		return m_partitionCapacity;
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
}
