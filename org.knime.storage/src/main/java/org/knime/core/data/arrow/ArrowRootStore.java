package org.knime.core.data.arrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.knime.core.data.arrow.struct.StructArrowStore;
import org.knime.core.data.arrow.vector.ArrowBitVectorFactory;
import org.knime.core.data.arrow.vector.ArrowDoubleVectorFactory;
import org.knime.core.data.arrow.vector.ArrowStringVectorFactory;
import org.knime.core.data.cache.SequentialCache;
import org.knime.core.data.store.RootStore;
import org.knime.core.data.store.Store;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.table.column.NativeType;

// TODO persistence done with one file per column at the moment. That's not working out will in case of very wide-data with only little rows. Also problematic as so many small files are created.
// TODO likely better approach: use parquet as persistence layer. can be done on implementation level without API changes.
class ArrowRootStore implements RootStore {

	private final ArrowStore<?>[] m_stores;
	private final RootAllocator m_allocator;

	// anticipation of future debugging :-)
	private UUID m_storeId = UUID.randomUUID();

	private Path m_baseDir;

	// TODO maybe we can have something like an adaptive batchSize at some point
	public ArrowRootStore(final long maxSize, final int batchSize, final ColumnSchema... schemas) {
		try {
			m_stores = new ArrowStore[schemas.length];
			m_allocator = new RootAllocator();
			m_baseDir = Files.createTempDirectory("ArrowStore_" + m_storeId.toString());

			// TODO check if there are smarter ways to do this in arrow than that
			for (int i = 0; i < schemas.length; i++) {
				final NativeType[] nativeTypes = schemas[i].getColumnType().getNativeTypes();
				if (nativeTypes.length == 1) {
					m_stores[i] = create(nativeTypes[0], "colIdx" + "_" + i + "_childIdx_" + 0, maxSize, batchSize, i,
							0);
				} else {
					final ArrowStore<?>[] stores = new ArrowStore[nativeTypes.length];
					for (int j = 0; j < nativeTypes.length; j++) {
						stores[j] = create(nativeTypes[j], "colIdx" + "_" + i + "_childIdx_" + j, maxSize, batchSize, i,
								j);
					}
					m_stores[i] = new StructArrowStore(stores);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private DefaultArrowStore<?> create(NativeType type, String name, long maxSize, int batchSize, int colIdx,
			int childIdx) throws IOException {
		// TODO no idea what a good init size or max-size is. Actually it should
		// type-dependent
		final BufferAllocator allocator = m_allocator.newChildAllocator(name, maxSize / (4 * m_stores.length), maxSize);
		switch (type) {
		// TODO a bit clunky. Util methods?
		case BOOLEAN:
			return new DefaultArrowStore<>(() -> new ArrowBitVectorFactory.BooleanArrowValue(),
					() -> new ArrowPartition<>(new ArrowBitVectorFactory(allocator, batchSize).create(), batchSize),
					new SequentialCache<>(new ArrowCacheFlusher<>(m_baseDir, name),
							new ArrowCacheLoader<>(m_baseDir, name, m_allocator)));
		case DOUBLE:
			return new DefaultArrowStore<>(() -> new ArrowDoubleVectorFactory.DoubleArrowValue(),
					() -> new ArrowPartition<>(new ArrowDoubleVectorFactory(allocator, batchSize).create(), batchSize),
					new SequentialCache<>(new ArrowCacheFlusher<>(m_baseDir, name),
							new ArrowCacheLoader<>(m_baseDir, name, m_allocator)));
		case STRING:
			return new DefaultArrowStore<>(() -> new ArrowStringVectorFactory.StringArrowValue(),
					() -> new ArrowPartition<>(new ArrowStringVectorFactory(allocator, batchSize).create(), batchSize),
					new SequentialCache<>(new ArrowCacheFlusher<>(m_baseDir, name),
							new ArrowCacheLoader<>(m_baseDir, name, m_allocator)));
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
