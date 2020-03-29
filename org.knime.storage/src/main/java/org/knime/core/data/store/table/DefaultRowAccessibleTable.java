package org.knime.core.data.store.table;

import org.knime.core.data.store.Value;
import org.knime.core.data.store.vec.VecAccess;
import org.knime.core.data.store.vec.VecAccessible;

// row access on vector accessible
public class DefaultRowAccessibleTable implements RowAccessibleBounded {

	private VecAccessible[] m_accessible;
	private long m_size;

	// TODO make VecAccessible and Size one interface..
	DefaultRowAccessibleTable(VecAccessible[] accessible, final long size) {
		m_accessible = accessible;
		m_size = size;
	}

	@Override
	public long size() {
		return m_size;
	}

	@Override
	public RowAccess access() {
		return new RowAccess() {

			// proxy row. reused.
			private final Row m_proxy = new Row() {

				@Override
				public Value valueAt(int i) {
					return m_vecAccess[i].get();
				}

				@Override
				public long numValues() {
					return m_vecAccess.length;
				}
			};

			private final VecAccess[] m_vecAccess;
			private int idx = -1;

			{
				m_vecAccess = new VecAccess[m_accessible.length];
				for (int i = 0; i < m_accessible.length; i++) {
					m_vecAccess[i] = m_accessible[i].access();
				}
			}

			@Override
			public void close() throws Exception {
				for (int i = 0; i < m_vecAccess.length; i++) {
					m_vecAccess[i].close();
				}
			}

			@Override
			public Row next() {
				fwd();
				return get();
			}

			@Override
			public boolean hasNext() {
				return idx < m_size - 1;
			}

			@Override
			public Row get() {
				return m_proxy;
			}

			@Override
			public void fwd() {
				idx++;
			}
		};
	}
}
