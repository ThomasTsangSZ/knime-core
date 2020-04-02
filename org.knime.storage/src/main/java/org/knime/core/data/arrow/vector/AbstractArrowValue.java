
package org.knime.core.data.arrow.vector;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.table.column.Partition;
import org.knime.core.data.table.column.PartitionValue;

// TODO composition over inheritance? :-(
abstract class AbstractArrowValue<V extends FieldVector> //
		implements PartitionValue<V> {

	protected int m_index = -1;

	protected V m_vector;

	@Override
	public void incIndex() {
		m_index++;
	}

	@Override
	public void updatePartition(final Partition<V> partition) {
		m_index = -1;
		m_vector = partition.get();
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
