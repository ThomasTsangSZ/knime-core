
package org.knime.core.data.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.table.value.ReadableDoubleValue;
import org.knime.core.data.table.value.WritableDoubleValue;

public final class ArrowDoubleColumnPartitionFactory extends AbstractArrowColumnPartitionFactory<Float8Vector> {

	public ArrowDoubleColumnPartitionFactory(final BufferAllocator allocator, final int partitionCapacity) {
		super(allocator, partitionCapacity);
	}

	@Override
	Float8Vector createStorageVector(final BufferAllocator allocator, final int capacity) {
		final Float8Vector vector = new Float8Vector((String) null, allocator);
		vector.allocateNew(capacity);
		return vector;
	}

	public static final class ArrowDoubleValueAccess //
		extends AbstractArrowPartitionedValueAccess<Float8Vector> //
		implements ReadableDoubleValue, WritableDoubleValue
	{

		@Override
		public double getDoubleValue() {
			return m_vector.get(m_index);
		}

		@Override
		public void setDoubleValue(final double value) {
			m_vector.set(m_index, value);
		}
	}
}
