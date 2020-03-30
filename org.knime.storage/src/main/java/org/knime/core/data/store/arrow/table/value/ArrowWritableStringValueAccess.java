
package org.knime.core.data.store.arrow.table.value;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.store.table.value.WritableStringValueAccess;

final class ArrowWritableStringValueAccess //
	extends AbstractArrowWritableValueAccess<VarCharVector> //
	implements WritableStringValueAccess
{

	@Override
	public void setStringValue(final String val) {
		// TODO: Is this correct? See knime-python's StringInserter which also
		// handles possible reallocations.
		m_vector.set(m_index, val.getBytes(StandardCharsets.UTF_8));
		m_vector.setValueCount(m_index + 1);
	}
}
