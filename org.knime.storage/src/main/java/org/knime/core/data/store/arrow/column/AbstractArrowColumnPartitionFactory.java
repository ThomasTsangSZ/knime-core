
package org.knime.core.data.store.arrow.column;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.column.partition.ColumnPartition;
import org.knime.core.data.store.column.partition.ColumnPartitionFactory;

public abstract class AbstractArrowColumnPartitionFactory<V extends FieldVector> implements ColumnPartitionFactory<V> {

	private final BufferAllocator m_allocator;

	private final int m_partitionCapacity;

	public AbstractArrowColumnPartitionFactory(final BufferAllocator allocator, final int partitionCapacity) {
		m_allocator = allocator;
		m_partitionCapacity = partitionCapacity;
	}

	@Override
	public ColumnPartition<V> createPartition() {
		return new ArrowColumnPartition<>(createStorageVector(m_allocator, m_partitionCapacity));
	}

	abstract V createStorageVector(BufferAllocator allocator, int capacity);
}
