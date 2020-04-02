
package org.knime.core.data.arrow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.knime.core.data.table.column.NativeType;
import org.knime.core.data.vector.AbstractRefManaged;
import org.knime.core.data.vector.Vector;
import org.knime.core.data.vector.WritableVectorStore;
import org.knime.core.data.vector.WritableVectorStoreFactory;
import org.knime.core.data.vector.cache.SequentialCache;
import org.knime.core.data.vector.group.WritableVectorStoreGroup;
import org.knime.core.data.vector.table.VectorValue;

// TODO: Common abstract calls with 'ReadableArrowVectorStoreRoot'
public final class WritableArrowVectorStoreRoot extends AbstractRefManaged implements WritableVectorStoreFactory<Long> {

	private final List<WritableVectorStore<Long, ?>> m_stores = new ArrayList<>();

	// TODO what's outside? what's inside?
	public WritableArrowVectorStoreRoot() {
	}

	@Override
	public WritableVectorStore<Long, ?> createWritableStore(NativeType... types) {
		final WritableVectorStore<Long, ?> storeForType;
		if (types.length == 1) {
			storeForType = createGroupForType(types[0]);
		} else {
			@SuppressWarnings("unchecked")
			final WritableVectorStore<Long, ?>[] struct = new WritableVectorStore[types.length];
			for (int i = 0; i < struct.length; i++) {
				struct[i] = createGroupForType(types[i]);
			}
			storeForType = new WritableVectorStoreGroup<>(struct);
		}
		m_stores.add(storeForType);
		return storeForType;
	}

	private <D extends FieldVector> DefaultWritableVectorStore<D> createGroupForType(NativeType type) {
		final ArrowNativeType<D> arrowType = ArrowUtils.create(type);
		final SequentialCache<Vector<D>> cache = new SequentialCache<>(arrowType.createFlusher());
		return new DefaultWritableVectorStore<D>(cache, () -> arrowType.createVectorValue(),
				() -> arrowType.createVector());
	}

	@Override
	public void flush() throws IOException {
		// TODO persist everything to disc. Q: Also free up all memory?
	}

	@Override
	protected void onAllReferencesReleased() {
		// TODO
	}

	class DefaultWritableVectorStore<D> implements WritableVectorStore<Long, D> {

		private long m_numVectors;
		private Supplier<VectorValue<D>> m_linkedValueSupplier;
		private Supplier<Vector<D>> m_vectorSupplier;
		private SequentialCache<Vector<D>> m_cache;

		DefaultWritableVectorStore(SequentialCache<Vector<D>> cache, Supplier<VectorValue<D>> linkedValueSupplier,
				Supplier<Vector<D>> vectorSupplier) {
			m_linkedValueSupplier = linkedValueSupplier;
			m_vectorSupplier = vectorSupplier;
			m_cache = cache;
		}

		@Override
		public VectorValue<D> createLinkedValue() {
			return m_linkedValueSupplier.get();
		}

		@Override
		public Pair<Long, Vector<D>> add() {
			final Vector<D> vector = m_vectorSupplier.get();
			return new ImmutablePair<Long, Vector<D>>(m_numVectors++, new Vector<D>() {

				@Override
				public D get() {
					return vector.get();
				}

				@Override
				public long getCapacity() {
					return vector.getCapacity();
				}

				@Override
				public void setNumValues(int numValues) {
					vector.setNumValues(numValues);
				}

				@Override
				public int getNumValues() {
					return vector.getNumValues();
				}

				@Override
				public void close() throws Exception {
					m_cache.add(vector);
				}
			});
		}
	}
}
