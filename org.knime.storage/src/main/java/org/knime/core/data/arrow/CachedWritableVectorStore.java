package org.knime.core.data.arrow;

import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.knime.core.data.vector.AbstractRefManaged;
import org.knime.core.data.vector.Vector;
import org.knime.core.data.vector.WritableVectorStore;
import org.knime.core.data.vector.cache.SequentialCache;
import org.knime.core.data.vector.table.VectorValue;

public class CachedWritableVectorStore<D> extends AbstractRefManaged implements WritableVectorStore<Long, D> {

	private SequentialCache<Vector<D>> m_linkedCache;
	private long m_numVectors = 0;

	// dynamic pieces
	private Supplier<VectorValue<D>> m_typeSupplier;
	private Supplier<Vector<D>> m_vectorSupplier;

	public CachedWritableVectorStore(SequentialCache<Vector<D>> cache, Supplier<VectorValue<D>> typeSupplier,
			Supplier<Vector<D>> vectorSupplier) {
		m_linkedCache = cache;
		m_typeSupplier = typeSupplier;
		m_vectorSupplier = vectorSupplier;
	}

	@Override
	public long numVectors() {
		return m_numVectors;
	}

	@Override
	public VectorValue<D> createLinkedValue() {
		return m_typeSupplier.get();
	}

	@Override
	protected void onAllReferencesReleased() {
		// TODO what to do with the cache here?
	}

	@Override
	public Pair<Long, Vector<D>> add() {
		// supplier already retains vector.
		final Vector<D> d = m_vectorSupplier.get();
		m_linkedCache.add(d);
		return new ImmutablePair<Long, Vector<D>>(m_numVectors++, d);
	}
}