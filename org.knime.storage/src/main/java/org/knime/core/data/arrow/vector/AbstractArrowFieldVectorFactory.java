
package org.knime.core.data.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.arrow.ArrowUtils;

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
		final V vector = create(m_allocator, m_minCapacity);
		// TODO for some reason vector has two references on it at that point. We'll
		// manually remove one to be consistent with our framework. We may want to open
		// a bug report @ Arrow. Likely, we're doing something wrong on our side in the
		// way we create vectors.
		ArrowUtils.releaseVector(vector);
		return vector;
	}

	abstract V create(BufferAllocator allocator, int capacity);

}
