package org.knime.core.data.store.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.BatchColumnAccessible;
import org.knime.core.data.store.BatchColumnAccessibleFactory;
import org.knime.core.data.store.BatchColumnReadAccess;
import org.knime.core.data.store.BatchColumnType;
import org.knime.core.data.store.BatchColumnWriteAccess;

public class ArrowColumnAccessibleFactory implements BatchColumnAccessibleFactory {

	private RootAllocator m_alloc;
	private int m_batchSize;

	ArrowColumnAccessibleFactory(int batchMaxSize, RootAllocator alloc) {
		m_batchSize = batchMaxSize;
		m_alloc = alloc;
	}

	// make extensible later?
	@Override
	public BatchColumnAccessible create(BatchColumnType type) {
		switch (type) {
		case BOOLEAN:
			return new BatchColumnAccessible() {
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
				public BatchColumnWriteAccess writeAccess() {
					// TODO
					return null;
				}

				@Override
				public BatchColumnReadAccess readAccess() {
					return new ArrowBooleanColumnReadAccess(m_vector);
				}
			};
		case STRING:
		case BYTE_ARRAY:
		case DOUBLE:
		case FLOAT:
			break;
		case INTEGER:
			break;
		default:
			throw new UnsupportedOperationException(type + " nyi");

		}
		return null;
	}
}
