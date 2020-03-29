package org.knime.core.data.store.vec;

import java.util.Iterator;

import org.knime.core.data.store.MutableDataValue;

public class VecAccessibleOnVecAccessibles implements VecAccessible {

	private VecSchema m_schema;

	private Iterable<VecAccessible> m_accessibles;

	public VecAccessibleOnVecAccessibles(VecSchema schema, Iterable<VecAccessible> accessibles) {
		m_schema = schema;
		m_accessibles = accessibles;
	}

	@Override
	public VecSchema schema() {
		return m_schema;
	}

	@Override
	public VecAccess access() {
		final Iterator<VecAccessible> it = m_accessibles.iterator();
		return new VecAccess() {
			private VecAccess curr = it.next().access();

			@Override
			public void fwd() {
				if (!curr.hasNext()) {
					try {
						curr.close();
						curr = it.next().access();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				} else {
					curr.fwd();
				}
			}

			@Override
			public boolean hasNext() {
				return curr.hasNext() || it.hasNext();
			}

			@Override
			public MutableDataValue[] get() {
				return curr.get();
			}

			@Override
			public MutableDataValue[] next() {
				fwd();
				return get();
			}

			@Override
			public void close() throws Exception {
				curr.close();
			}
		};
	}
}
