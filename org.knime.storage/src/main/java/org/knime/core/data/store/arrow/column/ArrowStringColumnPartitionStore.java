package org.knime.core.data.store.arrow.column;

import java.io.File;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.store.column.partition.ColumnPartitionValueAccess;
import org.knime.core.data.store.column.value.ReadableStringValueAccess;
import org.knime.core.data.store.column.value.WritableStringValueAccess;

public class ArrowStringColumnPartitionStore extends AbstractArrowColumnPartitionStore<VarCharVector> {

	public ArrowStringColumnPartitionStore(int batchSize, BufferAllocator allocator, File baseFile) {
		super(allocator, baseFile, batchSize);
	}

	@Override
	VarCharVector create(BufferAllocator alloc) {
		final VarCharVector vector = new VarCharVector((String) null, alloc);
		vector.allocateNew(64l * m_batchSize, m_batchSize);
		return vector;
	}

	@Override
	public ColumnPartitionValueAccess<VarCharVector> createAccess() {
		return new ArrowStringValueAccess();
	}

	final class ArrowStringValueAccess //
			extends AbstractArrowValueAccess<VarCharVector> //
			implements WritableStringValueAccess, ReadableStringValueAccess {

		@Override
		public String getStringValue() {
			// TODO: Is there a more efficient way? E.g. via m_vector.get(m_index) and
			// manual decoding.
			return m_vector.getObject(m_index).toString();
		}

		@Override
		public void setStringValue(String value) {
			// TODO: Is this correct? See knime-python's StringInserter which also
			// handles possible reallocations.
			m_vector.set(m_index, value.getBytes(StandardCharsets.UTF_8));
		}

	}

}
