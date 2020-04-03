package org.knime.core.data.arrow;

import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.knime.core.data.arrow.vector.BooleanArrowPartition;
import org.knime.core.data.arrow.vector.DoubleArrowPartition;
import org.knime.core.data.arrow.vector.StringArrowPartition;
import org.knime.core.data.cache.SequentialCache;
import org.knime.core.data.store.RootStore;
import org.knime.core.data.store.Store;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.table.column.NativeType;

class ArrowRootStore implements RootStore {

	private final ArrowStore<?>[] m_stores;
	private final RootAllocator m_allocator;

	// anticipation of future debugging :-)
	private UUID m_storeId;

	// TODO maybe we can have something like an adaptive batchSize at some point
	public ArrowRootStore(final long maxSize, final int batchSize, final ColumnSchema... schemas) {
		m_stores = new ArrowStore[schemas.length];
		m_allocator = new RootAllocator();

		// TODO check if there are smarter ways to do this in arrow than that
		for (int i = 0; i < schemas.length; i++) {
			final NativeType[] nativeTypes = schemas[i].getColumnType().getNativeTypes();
			if (nativeTypes.length == 1) {
				m_stores[i] = create(nativeTypes[0],
						m_storeId + " Store: " + schemas[i] + ", i ," + nativeTypes[0] + ",", maxSize, batchSize);
			} else {
				final ArrowStore<?>[] stores = new ArrowStore[nativeTypes.length];
				for (int j = 0; j < stores.length; j++) {
					stores[j] = create(nativeTypes[j],
							m_storeId + " Store: " + schemas[i] + ", i ," + nativeTypes[0] + ",", maxSize, batchSize);
				}
				m_stores[i] = new StructArrowStore(stores);
			}
		}
		m_storeId = UUID.randomUUID();
	}

	private DefaultArrowStore<?> create(NativeType type, String name, long maxSize, int batchSize) {
		// TODO no idea what a good init size or max-size is. Actually it should
		// type-dependent
		final BufferAllocator allocator = m_allocator.newChildAllocator(name, maxSize / (4 * m_stores.length), maxSize);
		switch (type) {
		case BOOLEAN:
			return new DefaultArrowStore<>(() -> new BooleanArrowPartition.BooleanArrowValue(),
					() -> new BooleanArrowPartition(allocator, batchSize), new SequentialCache<>(null, null));
		case DOUBLE:
			return new DefaultArrowStore<>(() -> new DoubleArrowPartition.DoubleArrowValue(),
					() -> new DoubleArrowPartition(allocator, batchSize), new SequentialCache<>(null, null));
		case STRING:
			return new DefaultArrowStore<>(() -> new StringArrowPartition.StringArrowValue(),
					() -> new StringArrowPartition(allocator, batchSize), new SequentialCache<>(null, null));
		default:
			throw new IllegalArgumentException("Unknown or not yet implemented NativeType " + type);
		}
	}

	@Override
	public long getNumStores() {
		return m_stores.length;
	}

	@Override
	public Store<?> getStoreAt(long index) {
		return m_stores[(int) index];
	}

	/* TODO define what flush means ('write to disc' and? 'release memory') */
	@Override
	public void flush() throws Exception {
		for (int i = 0; i < m_stores.length; i++) {
			m_stores[i].flush();
		}
	}

	@Override
	public void close() throws Exception {
		// release memory of stores
		for (int i = 0; i < m_stores.length; i++) {
			m_stores[i].close();
		}

		// close: release all memory. nobody will access it's contents anymore.
		for (final BufferAllocator alloc : m_allocator.getChildAllocators()) {
			alloc.close();
		}
		m_allocator.close();
	}

	@Override
	public void destroy() throws Exception {
		close();
		// delete all created files in case of writing.
	}

}
