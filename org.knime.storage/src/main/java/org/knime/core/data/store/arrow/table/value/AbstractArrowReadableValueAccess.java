
package org.knime.core.data.store.arrow.table.value;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.partition.ColumnPartitionReadableValueAccess;

public abstract class AbstractArrowReadableValueAccess<V extends ValueVector> implements
	ColumnPartitionReadableValueAccess<V>
{

	protected int m_index = -1;

	protected V m_vector;

	@Override
	public void incIndex() {
		m_index++;
	}

	@Override
	public void updatePartition(final V partition) {
		m_index = 0;
		m_vector = partition;
	}

	@Override
	public boolean isMissing() {
		return m_vector.isNull(m_index);
	}
}
