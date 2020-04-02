
package org.knime.core.data.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.knime.core.data.table.value.ReadableBooleanValue;
import org.knime.core.data.table.value.WritableBooleanValue;

public final class BooleanArrowPartition extends AbstractArrowPartition<BitVector> {

	public BooleanArrowPartition(final BufferAllocator allocator, final int partitionCapacity) {
		super(allocator, partitionCapacity);
	}

	@Override
	BitVector create(final BufferAllocator allocator, final int capacity) {
		final BitVector vector = new BitVector((String) null, allocator);
		vector.allocateNew(capacity);
		return vector;
	}

	public static final class BooleanArrowValue //
			extends AbstractArrowValue<BitVector> //
			implements ReadableBooleanValue, WritableBooleanValue {

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
