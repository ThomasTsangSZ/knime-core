
package org.knime.core.data.store;

import io.netty.buffer.ArrowBuf;

import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.table.column.ReadableColumn;
import org.knime.core.data.store.table.column.ReadableColumnIterator;
import org.knime.core.data.store.table.row.ReadableValueAccess;
import org.knime.core.data.store.vec.rw.ReadableDoubleValueAccess;

public class ArrowReadableDoubleColumn implements ReadableColumn {

	private final Float8Vector m_vector;

	ArrowReadableDoubleColumn(final Float8Vector vector) {
		m_vector = vector;
	}

	@Override
	public long size() {
		return m_vector.getValueCount();
	}

	@Override
	public ReadableColumnIterator iterator() {
		for (final ArrowBuf buffer : m_vector.getBuffers(false)) {
			buffer.getReferenceManager().retain();
		}
		return new ArrowReadableDoubleColumnIterator();
	}

	@Override
	public void close() throws Exception {
		m_vector.close();
	}

	private class ArrowReadableDoubleColumnIterator implements ReadableColumnIterator {

		private final ArrowReadableDoubleValueAccess m_access = new ArrowReadableDoubleValueAccess();

		private int m_index;

		@Override
		public boolean canFwd() {
			return m_index < m_vector.getValueCount() - 1;
		}

		@Override
		public void fwd() {
			m_index++;
		}

		@Override
		public ReadableValueAccess get() {
			return m_access;
		}

		@Override
		public void close() throws Exception {
			for (final ArrowBuf buffer : m_vector.getBuffers(false)) {
				buffer.getReferenceManager().release();
			}
		}

		private class ArrowReadableDoubleValueAccess implements ReadableDoubleValueAccess {

			@Override
			public boolean isMissing() {
				return m_vector.isNull(m_index);
			}

			@Override
			public double getDoubleValue() {
				return m_vector.get(m_index);
			}
		}
	}
}
