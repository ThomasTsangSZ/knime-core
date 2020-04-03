
package org.knime.core.data.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

// TODO composition over inheritance? :-(
abstract class AbstractArrowFieldVectorFactory<V extends FieldVector> implements ArrowFieldVectorFactory<V> {

	private final BufferAllocator m_allocator;

	private final int m_minCapacity;

	public AbstractArrowFieldVectorFactory(final BufferAllocator allocator, final int partitionCapacity) {
		m_allocator = allocator;
		m_minCapacity = partitionCapacity;
	}

	@Override
	public V create() {
		return create(m_allocator, m_minCapacity);
	}

	abstract V create(BufferAllocator allocator, int capacity);

}
