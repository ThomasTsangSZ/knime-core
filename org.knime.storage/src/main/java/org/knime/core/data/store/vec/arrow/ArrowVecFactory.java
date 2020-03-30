
package org.knime.core.data.store.vec.arrow;

import java.util.function.Function;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.store.table.column.ColumnType;
import org.knime.core.data.store.vec.VecAccessible;
import org.knime.core.data.store.vec.rw.VecFactory;
import org.knime.core.data.store.vec.rw.ReadableVectorAccess;
import org.knime.core.data.store.vec.rw.WritableVectorAccess;

public final class ArrowVecFactory implements VecFactory {

	private final RootAllocator m_alloc;

	private final int m_batchSize;

	ArrowVecFactory(final int batchMaxSize, final RootAllocator alloc) {
		m_batchSize = batchMaxSize;
		m_alloc = alloc;
	}

	// make extensible later?
	@Override
	public VecAccessible create(final ColumnType type) {
		switch (type) {
			case BOOLEAN: {
				final BitVector vector = new BitVector(type.toString(), m_alloc);
				vector.allocateNew(m_batchSize);
				return new ArrowVecAccessible<>(vector, ArrowBooleanVecWriteAccess::new, ArrowBooleanVecReadAccess::new);
			}
			case STRING: {
				final VarCharVector vector = new VarCharVector(type.toString(), m_alloc);
				// TODO more flexible configuration of "bytes per cell assumption".
				// E.g. rowIds might be smaller
				vector.allocateNew(64l * m_batchSize, m_batchSize);
				return new ArrowVecAccessible<>(vector, ArrowStringVecWriteAccess::new, ArrowStringVecReadAccess::new);
			}
			case DOUBLE: {
				final Float8Vector vector = new Float8Vector(type.toString(), m_alloc);
				vector.allocateNew(m_batchSize);
				return new ArrowVecAccessible<>(vector, ArrowDoubleVecWriteAccess::new, ArrowDoubleVecReadAccess::new);
			}
			default:
				throw new UnsupportedOperationException(type + " nyi");
		}
	}

	private static final class ArrowVecAccessible<V extends AutoCloseable> implements VecAccessible {

		private final V m_vector;

		private final Function<V, WritableVectorAccess> m_writeAccessConstructor;

		private final Function<V, ReadableVectorAccess> m_readAccessConstructor;

		private WritableVectorAccess m_writeAccess;

		public ArrowVecAccessible(final V vector, final Function<V, WritableVectorAccess> writeAccessConstructor,
			final Function<V, ReadableVectorAccess> readAccessConstructor)
		{
			m_vector = vector;
			m_writeAccessConstructor = writeAccessConstructor;
			m_readAccessConstructor = readAccessConstructor;
		}

		@Override
		public WritableVectorAccess getWriteAccess() {
			// TODO: This is not sufficient to prevent concurrent writes
			if (m_writeAccess == null) {
				m_writeAccess = m_writeAccessConstructor.apply(m_vector);
			}
			return m_writeAccess;
		}

		@Override
		public ReadableVectorAccess createReadAccess() {
			return m_readAccessConstructor.apply(m_vector);
		}

		@Override
		public void close() throws Exception {
			m_vector.close();
		}
	}
}
