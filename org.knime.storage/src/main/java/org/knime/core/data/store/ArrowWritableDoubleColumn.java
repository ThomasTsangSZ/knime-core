
package org.knime.core.data.store;

import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.table.row.WritableValueAccess;
import org.knime.core.data.store.vec.rw.WritableDoubleValueAccess;

public class ArrowWritableDoubleColumn extends AbstractArrowWritableColumn<Float8Vector> {

	private final ArrowWritableDoubleValueAccess m_access;

	ArrowWritableDoubleColumn(final VectorStore<Float8Vector> vectorStore) {
		super(vectorStore);
		m_access = new ArrowWritableDoubleValueAccess();
	}

	@Override
	public WritableValueAccess get() {
		return m_access;
	}

	// TODO IF this is a problem with performance that we have an additional
	// method
	// call, then this guy could implement WritableDoubleDataValue of KNIME
	private class ArrowWritableDoubleValueAccess implements WritableDoubleValueAccess {

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
