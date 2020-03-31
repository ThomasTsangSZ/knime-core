
package org.knime.core.data.store.arrow.table.value;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.partition.ColumnPartition;
import org.knime.core.data.store.partition.ColumnPartitionWritableValueAccess;

public abstract class AbstractArrowWritableValueAccess<V extends ValueVector>
		implements ColumnPartitionWritableValueAccess<V> {

	protected int m_index = -1;

	protected V m_vector;

	@Override
	public void incIndex() {
		m_index++;
	}

	@Override
	public void updatePartition(final ColumnPartition<V> partition) {
		m_index = 0;
		m_vector = partition.get();
	}

	@Override
	public void setMissing() {
		// TODO: Is this actually correct (especially when reusing the vector)? Or
		// use setNull instead? knime-python does it like here.
		m_vector.setValueCount(m_index + 1);
	}
}
