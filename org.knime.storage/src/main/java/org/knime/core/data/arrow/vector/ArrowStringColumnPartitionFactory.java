
package org.knime.core.data.arrow.vector;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.table.value.ReadableStringValue;
import org.knime.core.data.table.value.WritableStringValue;

public final class ArrowStringColumnPartitionFactory extends AbstractArrowVectorSupplier<VarCharVector> {

	public ArrowStringColumnPartitionFactory(final BufferAllocator allocator, final int partitionCapacity) {
		super(allocator, partitionCapacity);
	}

	@Override
	VarCharVector createStorageVector(final BufferAllocator allocator, final int capacity) {
		final VarCharVector vector = new VarCharVector((String) null, allocator);
		// TODO: Heuristic
		vector.allocateNew(64l * capacity, capacity);
		return vector;
	}

	public static final class ArrowStringValueAccess //
		extends AbstractArrowPartitionedValueAccess<VarCharVector> //
		implements ReadableStringValue, WritableStringValue
	{

		@Override
		public String getStringValue() {
			// TODO: Is there a more efficient way? E.g. via m_vector.get(m_index) and
			// manual decoding.
			return m_vector.getObject(m_index).toString();
		}

		@Override
		public void setStringValue(final String value) {
			// TODO: Is this correct? See knime-python's StringInserter which also
			// handles possible reallocations.
			m_vector.set(m_index, value.getBytes(StandardCharsets.UTF_8));
		}
	}
}
