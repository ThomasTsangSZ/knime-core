
package org.knime.core.data.store.arrow.column;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.column.partition.ColumnPartition;
import org.knime.core.data.store.column.partition.PartitionedReadableValueAccess;
import org.knime.core.data.store.column.partition.PartitionedWritableValueAccess;

public abstract class AbstractArrowPartitionedValueAccess<V extends FieldVector> //
	implements PartitionedReadableValueAccess<V>, PartitionedWritableValueAccess<V>
{

	protected int m_index = -1;

	protected V m_vector;

	@Override
	public void incIndex() {
		m_index++;
	}

	@Override
	public void updatePartition(final ColumnPartition<V> partition) {
		m_index = -1;
		m_vector = partition.getStorage();
	}

	@Override
	public boolean isMissing() {
		return m_vector.isNull(m_index);
	}

	@Override
	public void setMissing() {
		// TODO: Is this actually correct (especially when reusing the vector)? Or
		// use setNull instead? knime-python does it like here. But seems to be an
		// expensive operation!
		m_vector.setValueCount(m_index + 1);
	}
}
