package org.knime.core.data.store.arrow.column;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.column.value.ReadableDoubleValueAccess;
import org.knime.core.data.store.column.value.WritableDoubleValueAccess;

public class ArrowDoubleColumnPartitionFactory extends AbstractArrowColumnPartitionFactory<Float8Vector> {

	public ArrowDoubleColumnPartitionFactory(BufferAllocator allocator, int batchSize) {
		super(allocator, batchSize);
	}

	@Override
	Float8Vector create(BufferAllocator alloc, int size) {
		final Float8Vector vector = new Float8Vector((String) null, alloc);
		vector.allocateNew(size);
		return vector;
	}

	public static final class ArrowDoubleValueAccess //
			extends AbstractArrowValueAccess<Float8Vector> //
			implements WritableDoubleValueAccess, ReadableDoubleValueAccess {

		@Override
		public void setDoubleValue(final double value) {
			m_vector.set(m_index, value);
		}

		@Override
		public double getDoubleValue() {
			return m_vector.get(m_index);
		}
	}

}
