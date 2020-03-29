
package org.knime.core.data.store.vec.arrow;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.store.vec.rw.StringVecWriteAccess;

final class ArrowStringVecWriteAccess extends AbstractArrowVecWriteAccess<VarCharVector> implements
	StringVecWriteAccess
{

	public ArrowStringVecWriteAccess(final VarCharVector vector) {
		super(vector);
	}

	@Override
	public void setStringValue(final String val) {
		// TODO: Is this correct? See knime-python's StringInserter which also
		// handles possible reallocations.
		m_vector.set(m_idx, val.getBytes(StandardCharsets.UTF_8));
		m_vector.setValueCount(m_idx + 1);
	}
}
