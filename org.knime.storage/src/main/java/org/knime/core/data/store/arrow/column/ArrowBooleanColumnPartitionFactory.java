
package org.knime.core.data.store.arrow.column;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.column.value.ReadableBooleanValueAccess;
import org.knime.core.data.store.column.value.WritableBooleanValueAccess;

public final class ArrowBooleanColumnPartitionFactory extends AbstractArrowColumnPartitionFactory<BitVector> {

	public ArrowBooleanColumnPartitionFactory(final BufferAllocator allocator, final int partitionCapacity) {
		super(allocator, partitionCapacity);
	}

	@Override
	BitVector createStorageVector(final BufferAllocator allocator, final int capacity) {
		final BitVector vector = new BitVector((String) null, allocator);
		vector.allocateNew(capacity);
		return vector;
	}

	public static final class ArrowBooleanValueAccess //
		extends AbstractArrowPartitionedValueAccess<BitVector> //
		implements ReadableBooleanValueAccess, WritableBooleanValueAccess
	{

		@Override
		public boolean getBooleanValue() {
			return m_vector.get(m_index) > 0;
		}

		@Override
		public void setBooleanValue(final boolean value) {
			m_vector.set(m_index, value ? 1 : 0);
		}
	}
}
