
package org.knime.core.data.store;

import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.table.column.WritableColumn;
import org.knime.core.data.store.vec.rw.WritableDoubleValueAccess;

public class ArrowWritableDoubleColumn implements WritableColumn {

	private final Float8Vector m_vector;
	private final ArrowWritableDoubleValueAccess m_access;

	private int m_idx = 0;

	ArrowWritableDoubleColumn(final Float8Vector vector) {
		m_vector = vector;
		m_access = new ArrowWritableDoubleValueAccess();
	}

	@Override
	public void fwd() {
		m_idx++;
	}

	@Override
	public ArrowWritableDoubleValueAccess get() {
		return m_access;
	}

	// TODO IF this is a problem with performance that we have an additional
	// method
	// call, then this guy could implement WritableDoubleDataValue of KNIME
	class ArrowWritableDoubleValueAccess implements WritableDoubleValueAccess {

		// TODO is this actually correct? setNull?
		@Override
		public void setMissing() {
			m_vector.setValueCount(m_idx + 1);
		}

		@Override
		public void setDoubleValue(final double value) {
			m_vector.set(m_idx, value);
			m_vector.setValueCount(m_idx + 1);
		}
	}
}
