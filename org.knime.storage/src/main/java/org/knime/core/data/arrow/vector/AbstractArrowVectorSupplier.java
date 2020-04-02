
package org.knime.core.data.arrow.vector;

import java.util.function.Supplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.vector.Vector;

public abstract class AbstractArrowVectorSupplier<V extends FieldVector> implements Supplier<Vector<V>> {

	private final BufferAllocator m_allocator;

	private final int m_partitionCapacity;

	public AbstractArrowVectorSupplier(final BufferAllocator allocator, final int partitionCapacity) {
		m_allocator = allocator;
		m_partitionCapacity = partitionCapacity;
	}

	@Override
	public Vector<V> get() {
		return new ArrowVector<>(create(m_allocator, m_partitionCapacity));
	}

	abstract V create(BufferAllocator allocator, int capacity);
}
