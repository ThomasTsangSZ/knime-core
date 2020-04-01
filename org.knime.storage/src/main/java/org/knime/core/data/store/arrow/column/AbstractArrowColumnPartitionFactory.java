package org.knime.core.data.store.arrow.column;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.column.partition.ColumnPartition;
import org.knime.core.data.store.column.partition.ColumnPartitionFactory;

abstract class AbstractArrowColumnPartitionFactory<V extends FieldVector> implements ColumnPartitionFactory<V> {

	private final BufferAllocator m_alloc;
	private final int m_batchSize;

	protected AbstractArrowColumnPartitionFactory(BufferAllocator alloc, int batchSize) {
		m_alloc = alloc;
		m_batchSize = batchSize;
	}

	@Override
	public ColumnPartition<V> appendPartition() {
		return new ArrowColumnPartition<V>(create(m_alloc, m_batchSize));
	}

	abstract V create(BufferAllocator alloc, int size);

}
