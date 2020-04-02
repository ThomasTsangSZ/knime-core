package org.knime.core.data.arrow;

import java.io.IOException;
import java.util.function.Supplier;

import org.knime.core.data.vector.AbstractRefManaged;
import org.knime.core.data.vector.ReadableVectorStore;
import org.knime.core.data.vector.Vector;
import org.knime.core.data.vector.cache.SequentialCache;
import org.knime.core.data.vector.cache.SequentialCacheLoader;
import org.knime.core.data.vector.table.VectorValue;

public class CachedReadableVectorStore<D> extends AbstractRefManaged implements ReadableVectorStore<Long, D> {

	private long m_numVectors;
	private Supplier<VectorValue<D>> m_valueSupplier;
	private SequentialCache<Vector<D>> m_cache;
	private SequentialCacheLoader<Vector<D>> m_loader;

	public CachedReadableVectorStore(long numVectors, Supplier<VectorValue<D>> valueSupplier,
			SequentialCache<Vector<D>> cache, SequentialCacheLoader<Vector<D>> loader) {
		m_numVectors = numVectors;
		m_valueSupplier = valueSupplier;
		m_cache = cache;
		m_loader = loader;
	}

	@Override
	public long numVectors() {
		return m_numVectors;
	}

	@Override
	public VectorValue<D> createLinkedValue() {
		return m_valueSupplier.get();
	}

	@Override
	protected void onAllReferencesReleased() {
		// TODO Clear cache, memory and all created files.
	}

	@Override
	public Vector<D> get(Long key) {
		try {
			return m_cache.get(key, m_loader);
		} catch (IOException e) {
			// TODO exception handling
			throw new RuntimeException(e);
		}
	}
}
