package org.knime.core.data.store.table;

import org.knime.core.data.store.Value;
import org.knime.core.data.store.vec.VecReadAccess;
import org.knime.core.data.store.vec.VecReadAccessible;

// row access on vector accessible
// TODO generalize row-independent code and move to store.vec

// TODO we didn't implement "destroy" here. might be done by a wrapper.
public class DefaultTableAccessible<V extends Value> implements TableAccessible<V> {

	private VecReadAccessible<V> m_accessible;

	DefaultTableAccessible(VecReadAccessible<V> accessible) {
		m_accessible = accessible;
	}

	@Override
	public TableAccess<V> access() {
		return new TableAccess<V>() {
			private final VecReadAccess<V> m_vecAccess = m_accessible.access();
			private final V[] vecAccesses = m_vecAccess.get();

			private long idx = -1;

			// proxy row. reused.
			private final Row<V> m_proxy = new Row<V>() {

				@Override
				public V valueAt(int i) {
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
			public Row<V> next() {
				fwd();
				return get();
			}

			@Override
			public boolean hasNext() {
				return idx < Long.MAX_VALUE;
			}

			@Override
			public Row<V> get() {
				return m_proxy;
			}

			@Override
			public void fwd() {
				idx++;
			}
		};
	}
}
