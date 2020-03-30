
package org.knime.core.data.store.arrow.table.value;

import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.table.value.ReadableDoubleValueAccess;

public final class ArrowReadableDoubleValueAccess //
	extends AbstractArrowReadableValueAccess<Float8Vector> //
	implements ReadableDoubleValueAccess
{

	@Override
	public double getDoubleValue() {
		return m_vector.get(m_index);
	}
}
