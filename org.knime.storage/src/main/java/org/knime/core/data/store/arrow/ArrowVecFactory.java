package org.knime.core.data.store.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.store.vec.Vec;
import org.knime.core.data.store.vec.VecFactory;
import org.knime.core.data.store.vec.VecReadAccess;
import org.knime.core.data.store.vec.VecType;
import org.knime.core.data.store.vec.VecWriteAccess;

public class ArrowVecFactory implements VecFactory {

	private RootAllocator m_alloc;
	private int m_batchSize;

	ArrowVecFactory(int batchMaxSize, RootAllocator alloc) {
		m_batchSize = batchMaxSize;
		m_alloc = alloc;
	}

	// make extensible later?
	@Override
	public Vec create(VecType type) {
		switch (type) {
		case BOOLEAN:
			return new Vec() {
				private BitVector m_vector;
				{
					// TODO better name for vector.
					m_vector = new BitVector(type.toString(), m_alloc);
					m_vector.allocateNew(m_batchSize);
				}

				@Override
				public void close() throws Exception {
					m_vector.close();
				}

				@Override
				public VecWriteAccess writeAccess() {
					return new ArrowBooleanVecWriteAccess(m_vector);
				}

				@Override
				public VecReadAccess readAccess() {
					return new ArrowBooleanVecReadAccess(m_vector);
				}
			};
		case STRING:
			return new Vec() {
				final VarCharVector m_vector;
				{
					m_vector = new VarCharVector(type.toString(), m_alloc);
					// TODO more flexible configuration of "bytes per cell assumption". E.g. rowIds
					// might be smaller
					m_vector.allocateNew(64l * m_batchSize, m_batchSize);
				}

				@Override
				public void close() throws Exception {
					m_vector.close();
				}

				@Override
				public VecWriteAccess writeAccess() {
					return new ArrowStringVecWriteAccess(m_vector);
				}

				@Override
				public VecReadAccess readAccess() {
					return new ArrowStringVecReadAccess(m_vector);
				}
			};
		case DOUBLE:
			return new Vec() {
				final Float8Vector m_vector;
				{
					m_vector = new Float8Vector(type.toString(), m_alloc);
					m_vector.allocateNew(m_batchSize);
				}

				@Override
				public void close() throws Exception {
					m_vector.close();
				}

				@Override
				public VecReadAccess readAccess() {
					return new ArrowDoubleVecReadAccess(m_vector);
				}

				@Override
				public VecWriteAccess writeAccess() {
					return new ArrowDoubleVecWriteAccess(m_vector);
				}
			};
		default:
			throw new UnsupportedOperationException(type + " nyi");

		}
	}
}
