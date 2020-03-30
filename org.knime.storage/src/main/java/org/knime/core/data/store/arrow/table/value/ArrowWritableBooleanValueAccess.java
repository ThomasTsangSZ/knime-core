
package org.knime.core.data.store.arrow.table.value;

import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.table.value.WritableBooleanValueAccess;

public final class ArrowWritableBooleanValueAccess //
	extends AbstractArrowWritableValueAccess<BitVector> //
	implements WritableBooleanValueAccess
{

	@Override
	public void setBooleanValue(final boolean value) {
		m_vector.set(m_index, value ? 1 : 0);
		m_vector.setValueCount(m_index + 1);
	}
}
