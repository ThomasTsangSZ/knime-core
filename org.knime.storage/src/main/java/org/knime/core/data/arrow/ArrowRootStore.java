package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.knime.core.data.arrow.struct.StructArrowPartitionStore;
import org.knime.core.data.arrow.vector.ArrowBitVectorFactory;
import org.knime.core.data.arrow.vector.ArrowDoubleVectorFactory;
import org.knime.core.data.arrow.vector.ArrowStringVectorFactory;
import org.knime.core.data.cache.SequentialCache;
import org.knime.core.data.partition.PartitionStore;
import org.knime.core.data.partition.Store;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.table.column.NativeType;

// TODO persistence done with one file per column at the moment. That's not working out will in case of very wide-data with only little rows. Also problematic as so many small files are created.
// TODO likely better approach: use parquet as persistence layer. can be done on implementation level without API changes.
class ArrowRootStore implements Store {

	private final ArrowPartitionStore<?>[] m_stores;
	private final RootAllocator m_allocator;

	// anticipation of future debugging :-)
	private UUID m_storeId = UUID.randomUUID();

	private Path m_baseDir;

	// TODO maybe we can have something like an adaptive batchSize at some point
	// TODO documentation that vectorCapacity will be adopted.
	public ArrowRootStore(final long maxSize, final int batchSize, final ColumnSchema... schemas) {
		final int vectorCapacity = RootAllocator.nextPowerOfTwo(batchSize);
		try {
			m_stores = new ArrowPartitionStore[schemas.length];
			m_allocator = new RootAllocator();
			m_baseDir = Files.createTempDirectory("ArrowStore_" + m_storeId.toString());

			// TODO check if there are smarter ways to do this in arrow than that
			for (int i = 0; i < schemas.length; i++) {
				final NativeType[] nativeTypes = schemas[i].getColumnType().getNativeTypes();
				if (nativeTypes.length == 1) {
					m_stores[i] = create(nativeTypes[0], "colIdx" + "_" + i + "_childIdx_" + 0, maxSize, vectorCapacity,
							i, 0);
				} else {
					final ArrowPartitionStore<?>[] stores = new ArrowPartitionStore[nativeTypes.length];
					for (int j = 0; j < nativeTypes.length; j++) {
						stores[j] = create(nativeTypes[j], "colIdx" + "_" + i + "_childIdx_" + j, maxSize,
								vectorCapacity, i, j);
					}
					m_stores[i] = new StructArrowPartitionStore(i, stores);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private DefaultArrowPartitionStore<?> create(NativeType type, String name, long maxSize, int vectorCapacity,
			int colIdx, int childIdx) throws IOException {
		// TODO make sure that vectorCapacity is toThePowerOf2

		// TODO no idea what a good init size or max-size is. Actually it should
		// type-dependent
		final BufferAllocator allocator = m_allocator.newChildAllocator(name, maxSize / (4 * m_stores.length), maxSize);
		final File f = new File(m_baseDir.toFile(), "ColumnIdx_" + colIdx + "ChildIdx " + childIdx + ".knarrow");
		switch (type) {

		// TODO a bit clunky. Util methods?
		// TODO last argument for DefaultPartitionStore (batchSize) could also be
		// retrieved from VectorFactory.
		case BOOLEAN:
			return new DefaultArrowPartitionStore<>(() -> new ArrowBitVectorFactory.BooleanArrowValue(),
					() -> new ArrowBitVectorFactory(allocator, vectorCapacity).create(),
					new SequentialCache<>(new ArrowCacheFlusher<>(f), new ArrowCacheLoader<>(f, allocator)));
		case DOUBLE:
			return new DefaultArrowPartitionStore<>(() -> new ArrowDoubleVectorFactory.DoubleArrowValue(),
					() -> new ArrowDoubleVectorFactory(allocator, vectorCapacity).create(),
					new SequentialCache<>(new ArrowCacheFlusher<>(f), new ArrowCacheLoader<>(f, m_allocator
							.newChildAllocator(name + "FLUSHER", maxSize / (4 * m_stores.length), maxSize))));
		case STRING:
			return new DefaultArrowPartitionStore<>(() -> new ArrowStringVectorFactory.StringArrowValue(),
					() -> new ArrowStringVectorFactory(allocator, vectorCapacity).create(),
					new SequentialCache<>(new ArrowCacheFlusher<>(f), new ArrowCacheLoader<>(f, allocator)));
		default:
			throw new IllegalArgumentException("Unknown or not yet implemented NativeType " + type);
		}
	}

	@Override
	public long getNumStores() {
		return m_stores.length;
	}

	@Override
	public PartitionStore<?> getStoreAt(long index) {
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
}
