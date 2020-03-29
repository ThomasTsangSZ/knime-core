package org.knime.core.data.store.arrow;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.vector.VarCharVector;
import org.knime.core.data.store.vec.rw.StringVecWriteAccess;

public class ArrowStringVecWriteAccess extends AbstractArrowVecWriteAccess<VarCharVector>
		implements StringVecWriteAccess {

	protected ArrowStringVecWriteAccess(VarCharVector vector) {
		super(vector);
	}

	@Override
	public void setMissing() {
		m_vector.setNull(m_idx);
	}

	@Override
	public void set(String val) {
		m_vector.set(m_idx, val.getBytes(StandardCharsets.UTF_8));

	}

}
