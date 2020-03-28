package org.knime.core.data.store.arrow;

import org.knime.core.data.store.Batch;
import org.knime.core.data.store.BatchColumnAccessible;
import org.knime.core.data.store.BatchColumnReadAccess;
import org.knime.core.data.store.BatchColumnWriteAccess;
import org.knime.core.data.store.BatchSpec;

public class ArrowInMemoryBatch implements Batch {

	private final BatchSpec m_spec;
	private final BatchColumnAccessible[] m_accessibles;

	public ArrowInMemoryBatch(final ArrowColumnAccessibleFactory fac, BatchSpec spec) {
		m_spec = spec;
		m_accessibles = new BatchColumnAccessible[spec.getNumColumns()];
		for (int colIdx = 0; colIdx < m_accessibles.length; colIdx++) {
			m_accessibles[colIdx] = fac.create(spec.getTypeAt(colIdx));
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
	public BatchSpec getSpec() {
		return m_spec;
	}

	@Override
	public BatchColumnReadAccess getReadAccessAt(int idx) {
		return m_accessibles[idx].readAccess();
	}

	@Override
	public BatchColumnWriteAccess getWriteAccessAt(int idx) {
		return m_accessibles[idx].writeAccess();
	}
}
