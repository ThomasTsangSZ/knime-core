package org.knime.core.data.store.table;

import org.knime.core.data.store.Value;
import org.knime.core.data.store.vec.VecReadAccessible;

public class DefaultTableAccessibleBounded extends DefaultTableAccessible<Value>
		implements TableAccessibleBounded<Value> {

	private long m_size;

	DefaultTableAccessibleBounded(VecReadAccessible<Value> accessible, long size) {
		super(accessible);
		m_size = size;
	}

	@Override
	public long size() {
		return m_size;
	}

}
