package org.knime.core.data.store.table;

import org.knime.core.data.store.MutableValue;
import org.knime.core.data.store.vec.VecAccess;
import org.knime.core.data.store.vec.VecAccessible;

// row access on vector accessible
// TODO generalize row-independent code and move to store.vec

// TODO we didn't implement "destroy" here. might be done by a wrapper.
public class DefaultTableAccessible implements TableAccessible<MutableValue> {

	private VecAccessible m_accessible;

	DefaultTableAccessible(VecAccessible accessible) {
		m_accessible = accessible;
	}

	@Override
	public TableAccess<MutableValue> access() {
		return new TableAccess<MutableValue>() {
			private final VecAccess m_vecAccess = m_accessible.access();
			private final MutableValue[] vecAccesses = m_vecAccess.get();

			private long idx = -1;

			// proxy row. reused.
			private final Row<MutableValue> m_proxy = new Row<MutableValue>() {

				@Override
				public MutableValue valueAt(int i) {
					return vecAccesses[i];
				}

				@Override
				public long numValues() {
					return vecAccesses.length;
				}
			};

			@Override
			public void close() throws Exception {
				m_vecAccess.close();
			}

			@Override
			public Row<MutableValue> next() {
				fwd();
				return get();
			}

			@Override
			public boolean hasNext() {
				return idx < Long.MAX_VALUE;
			}

			@Override
			public Row<MutableValue> get() {
				return m_proxy;
			}

			@Override
			public void fwd() {
				idx++;
			}
		};
	}
}
