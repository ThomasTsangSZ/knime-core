package org.knime.core.data.store.arrow.table.value;

import java.io.File;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.partition.ColumnPartitionValueAccess;
import org.knime.core.data.store.table.value.ReadableDoubleValueAccess;
import org.knime.core.data.store.table.value.WritableDoubleValueAccess;

public class ArrowDoubleColumnPartitionStore extends AbstractArrowColumnPartitionStore<Float8Vector> {

	public ArrowDoubleColumnPartitionStore(int batchSize, BufferAllocator allocator, File baseDir) {
		super(allocator, baseDir, batchSize);
	}

	@Override
	Float8Vector create(BufferAllocator alloc) {
		final Float8Vector vector = new Float8Vector((String) null, alloc);
		vector.allocateNew(m_batchSize);
		return vector;
	}

	@Override
	public ColumnPartitionValueAccess<Float8Vector> createAccess() {
		return new ArrowDoubleValueAccess();
	}

	final class ArrowDoubleValueAccess //
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
