
package org.knime.core.data.arrow;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.table.column.vector.VectorWritableValue;
import org.knime.core.data.vector.Vector;
import org.knime.core.data.vector.table.VectorValue;

public abstract class AbstractArrowPartitionedValueAccess<V extends FieldVector> //
	implements VectorValue<V>, VectorWritableValue<V>
{

	protected int m_index = -1;

	protected V m_vector;

	@Override
	public void incIndex() {
		m_index++;
	}

	@Override
	public void updatePartition(final Vector<V> partition) {
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
