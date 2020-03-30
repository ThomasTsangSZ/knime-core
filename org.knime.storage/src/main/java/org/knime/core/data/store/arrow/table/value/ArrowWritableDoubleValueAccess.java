
package org.knime.core.data.store.arrow.table.value;

import org.apache.arrow.vector.Float8Vector;
import org.knime.core.data.store.table.value.WritableDoubleValueAccess;

// TODO: If it is a problem with performance that we have an additional level of
// indirection, then this and other Arrow-native accesses could implement their
// respective KNIME DataValue directly instead of being wrapped by one.
public final class ArrowWritableDoubleValueAccess //
	extends AbstractArrowWritableValueAccess<Float8Vector> //
	implements WritableDoubleValueAccess
{

	@Override
	public void setDoubleValue(final double value) {
		m_vector.set(m_index, value);
		m_vector.setValueCount(m_index + 1);
	}
}
