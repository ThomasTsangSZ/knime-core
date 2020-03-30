package org.knime.core.data.store;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.vec.rw.WritableDoubleValueAccess;

public class ArrowWritableDoubleColumn implements WritableColumn {

	private Float8Vector m_vector;
	private ArrowWritableDoubleValueAccess m_access;

	private int m_idx = 0;

	ArrowWritableDoubleColumn(RootAllocator alloc, int initialBatchSize) {
		m_vector = new Float8Vector("TODO", alloc);
		m_vector.allocateNew(initialBatchSize);
		m_access = new ArrowWritableDoubleValueAccess();
	}

	@Override
	public void close() throws Exception {
		m_vector.close();
	}

	@Override
	public void fwd() {
		m_idx++;
	}

	@Override
	public ArrowWritableDoubleValueAccess get() {
		return m_access;
	}

	// TODO IF this is a problem with performance that we have an additional method
	// call, then this guy could implement WritableDoubleDataValue of KNIME
	class ArrowWritableDoubleValueAccess implements WritableDoubleValueAccess {

		// TODO is this actually correct? setNull?
		@Override
		public void setMissing() {
			m_vector.setValueCount(m_idx + 1);
		}

		@Override
		public void setDoubleValue(double value) {
			m_vector.set(m_idx, value);
			m_vector.setValueCount(m_idx + 1);
		}
	}
}
