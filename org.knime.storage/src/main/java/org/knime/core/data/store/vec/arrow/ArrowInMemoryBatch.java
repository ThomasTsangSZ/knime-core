package org.knime.core.data.store.vec.arrow;

import org.knime.core.data.store.chunk.Chunk;
import org.knime.core.data.store.vec.VecReadAccessible;
import org.knime.core.data.store.vec.VecSchema;
import org.knime.core.data.store.vec.rw.VecReadAccess;
import org.knime.core.data.store.vec.rw.VecWriteAccess;

public class ArrowInMemoryBatch implements Chunk {

	private final VecSchema m_spec;
	private final VecReadAccessible[] m_accessibles;

	public ArrowInMemoryBatch(final ArrowVecFactory fac, VecSchema spec) {
		m_spec = spec;
		m_accessibles = new VecReadAccessible[spec.getNumVecs()];
		for (int colIdx = 0; colIdx < m_accessibles.length; colIdx++) {
			m_accessibles[colIdx] = fac.create(spec.getVecTypeAt(colIdx));
		}
	}

	// close means: killed.
	@Override
	public void close() throws Exception {
		// TODO do we have to close writers if we also close schema root?
		for (int i = 0; i < m_accessibles.length; i++) {
			m_accessibles[i].close();
		}
	}

	@Override
	public VecSchema getSchema() {
		return m_spec;
	}

	@Override
	public VecReadAccess getReadAccessAt(int idx) {
		return m_accessibles[idx].readAccess();
	}

	@Override
	public VecWriteAccess getWriteAccessAt(int idx) {
		return m_accessibles[idx].writeAccess();
	}
}
