package org.knime.core.data.store.arrow.column;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.column.value.ReadableBooleanValueAccess;
import org.knime.core.data.store.column.value.WritableBooleanValueAccess;

public class ArrowBooleanColumnPartitionFactory extends AbstractArrowColumnPartitionFactory<BitVector> {

	public ArrowBooleanColumnPartitionFactory(BufferAllocator allocator, int size) {
		super(allocator, size);
	}

	@Override
	BitVector create(BufferAllocator alloc, int maxSize) {
		final BitVector vector = new BitVector((String) null, alloc);
		vector.allocateNew(maxSize);
		return vector;
	}

	public static final class ArrowBooleanValueAccess //
			extends AbstractArrowValueAccess<BitVector> //
			implements WritableBooleanValueAccess, ReadableBooleanValueAccess {

		@Override
		public boolean getBooleanValue() {
			return m_vector.get(m_index) > 0;
		}

		@Override
		public void setBooleanValue(boolean value) {
			m_vector.set(m_index, value ? 1 : 0);
		}
	}

}
