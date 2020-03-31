
package org.knime.core.data.store.arrow.table.value;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.partition.ColumnPartition;
import org.knime.core.data.store.partition.ColumnPartitionReadableValueAccess;

public abstract class AbstractArrowReadableValueAccess<V extends FieldVector>
		implements ColumnPartitionReadableValueAccess<V> {

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
	public boolean isMissing() {
		return m_vector.isNull(m_index);
	}
}
