package org.knime.core.data.store.vec;

import java.util.Iterator;

import org.knime.core.data.store.Value;

public class VecAccessibleOnVecAccessibles<V extends Value, A extends VecAccessible<V, ?>>
		implements VecAccessible<V, A> {

	private VecSchema m_schema;
	private Iterable<VecReadAccessible<V>> m_accessibles;

	public VecAccessibleOnVecAccessibles(VecSchema schema, Iterable<VecReadAccessible<V>> accessibles) {
		m_schema = schema;
		m_accessibles = accessibles;
	}

	@Override
	public VecSchema schema() {
		return m_schema;
	}

	@Override
	public VecReadAccess<V> access() {
		final Iterator<VecReadAccessible<V>> it = m_accessibles.iterator();
		return new VecReadAccess<V>() {
			private VecReadAccess<V> curr = it.next().access();

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
			public V[] get() {
				return curr.get();
			}

			@Override
			public V[] next() {
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
