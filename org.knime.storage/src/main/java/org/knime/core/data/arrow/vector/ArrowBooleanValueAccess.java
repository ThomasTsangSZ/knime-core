package org.knime.core.data.arrow.vector;

import org.apache.arrow.vector.BitVector;
import org.knime.core.data.table.value.ReadableBooleanValue;
import org.knime.core.data.table.value.WritableBooleanValue;

public final class ArrowBooleanValueAccess //
		extends AbstractArrowPartitionedValueAccess<BitVector> //
		implements ReadableBooleanValue, WritableBooleanValue {

	@Override
	public boolean getBooleanValue() {
		return m_vector.get(m_index) > 0;
	}

	@Override
	public void setBooleanValue(final boolean value) {
		m_vector.set(m_index, value ? 1 : 0);
	}
}