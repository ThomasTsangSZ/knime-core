
package org.knime.core.data.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;

public final class BooleanArrowVectorSupplier extends AbstractArrowVectorSupplier<BitVector> {

	public BooleanArrowVectorSupplier(final BufferAllocator allocator, final int partitionCapacity) {
		super(allocator, partitionCapacity);
	}

	@Override
	BitVector create(final BufferAllocator allocator, final int capacity) {
		final BitVector vector = new BitVector((String) null, allocator);
		vector.allocateNew(capacity);
		return vector;
	}

}
