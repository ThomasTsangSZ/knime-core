
package org.knime.core.data.store.arrow.table.value;

import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.store.table.value.ReadableStringValueAccess;

public final class ArrowReadableStringValueAccess //
	extends AbstractArrowReadableValueAccess<VarCharVector> //
	implements ReadableStringValueAccess
{

	@Override
	public String getStringValue() {
		// TODO: Is there a more efficient way? E.g. via m_vector.get(m_index) and
		// manual decoding.
		return m_vector.getObject(m_index).toString();
	}
}
