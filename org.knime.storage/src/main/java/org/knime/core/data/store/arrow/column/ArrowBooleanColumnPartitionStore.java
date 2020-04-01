package org.knime.core.data.store.arrow.column;

import java.io.File;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.column.partition.ColumnPartitionValueAccess;
import org.knime.core.data.store.column.value.ReadableBooleanValueAccess;
import org.knime.core.data.store.column.value.WritableBooleanValueAccess;

public class ArrowBooleanColumnPartitionStore extends AbstractArrowColumnPartitionStore<BitVector> {

	public ArrowBooleanColumnPartitionStore(int batchSize, BufferAllocator allocator, File baseFile) {
		super(allocator, baseFile, batchSize);
	}

	@Override
	BitVector create(BufferAllocator alloc) {
		final BitVector vector = new BitVector((String) null, alloc);
		vector.allocateNew(m_batchSize);
		return vector;
	}

	@Override
	public ColumnPartitionValueAccess<BitVector> createAccess() {
		return new ArrowBooleanValueAccess();
	}

	final class ArrowBooleanValueAccess //
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
