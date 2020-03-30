
package org.knime.core.data.store.arrow.table.value;

import org.apache.arrow.vector.BitVector;
import org.knime.core.data.store.table.value.ReadableBooleanValueAccess;

public final class ArrowReadableBooleanValueAccess //
	extends AbstractArrowReadableValueAccess<BitVector> //
	implements ReadableBooleanValueAccess
{

	@Override
	public boolean getBooleanValue() {
		return m_vector.get(m_index) > 0;
	}
}
