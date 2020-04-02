
package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.data.table.column.NativeType;
import org.knime.core.data.vector.AbstractRefManaged;
import org.knime.core.data.vector.FlushableVectorStore;
import org.knime.core.data.vector.RefManaged;
import org.knime.core.data.vector.Vector;
import org.knime.core.data.vector.ReadableVectorStore;
import org.knime.core.data.vector.VectorStoreGroup;
import org.knime.core.data.vector.cache.CacheFlusher;
import org.knime.core.data.vector.cache.SequentialCache;
import org.knime.core.data.vector.cache.SequentialCacheLoader;
import org.knime.core.data.vector.table.VectorValue;

// TODO: Yeah I know, the class contains "Table" in its name. But it's really
// only there to have a central place that holds the allocator and creates the
// vectors and their files in a common base directory ;-).
public final class ArrowVectorStoreRoot<V extends RefManaged> extends AbstractRefManaged
		implements FlushableVectorStore<Long> {

	private final File m_baseDirectory;

	private final Schema[] m_schema;

	private final BufferAllocator m_allocator;

	private final List<ReadableVectorStore<Long, ? extends V>> m_createdStores = new ArrayList<>();

	// TODO what's outside? what's inside?
	public ArrowVectorStoreRoot() {
		m_baseDirectory = null;
		m_schema = null;
		m_allocator = null;
	}

	// TODO For some backends it may make sense to handle structs more efficiently
	// themselves. Can we let them decide how to handle structs?
	@Override
	public ReadableVectorStore<Long, ? extends V> createReadableStore(NativeType... types) {
		final ReadableVectorStore<Long, ? extends V> storeForType;
		if (types.length == 1) {
			storeForType = createGroupForType(types[0]);
		} else {
			@SuppressWarnings("unchecked")
			final ReadableVectorStore<Long, ? extends V>[] struct = new ReadableVectorStore[types.length];
			for (int i = 0; i < struct.length; i++) {
				struct[i] = createGroupForType(types[i]);
			}
			storeForType = new VectorStoreGroup<>(struct);
		}
		m_createdStores.add(storeForType);
		return storeForType;
	}

	private ReadableVectorStore<Long, ? extends V> createGroupForType(NativeType type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void flush() throws IOException {
		// TODO persist everything to disc. Q: Also free up all memory?
	}

	@Override
	protected void onAllReferencesReleased() {
		// release own reference to all groups. groups will always exists as long as the
		// store exists.
		m_createdStores.forEach(g -> g.release());
	}

	private void write(V vector) {

	}

	class ArrowVectorStore<D extends V> extends AbstractRefManaged implements ReadableVectorStore<Long, D> {

		private final SequentialCache<D> CACHE = new SequentialCache<D>(new CacheFlusher<D>() {
			@Override
			public void flush(D obj) throws IOException {
				write(obj);
			}
		});

		public ArrowVectorStore() {
		}

		@Override
		public Vector<D> getOrCreate(Long key) {
			try {
				CACHE.get(key, new SequentialCacheLoader<D>() {
					@Override
					public D load(long index) throws IOException {
						
						// 1) read from disc
						// 2) create new
						
						return null;
					}
				});
			} catch (IOException e) {
				// TODO Exception handling
				throw new RuntimeException(e);
			}

			return null;
		}

		@Override
		public long numVectors() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public VectorValue<D> createLinkedValue() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected void onAllReferencesReleased() {
			// TODO Clear cache, memory and all created files.
		}
	}

//	private ArrowColumnAccess<? extends FieldVector> addColumn(final ColumnType type) {
//	final BufferAllocator childAllocator = m_rootAllocator.newChildAllocator("ChildAllocator", 0, m_rootAllocator
//		.getLimit());
//	switch (type) {
//		case BOOLEAN:
//			return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.Binary(), childAllocator,
//				new ArrowBooleanColumnPartitionFactory(childAllocator, m_batchSize), () -> new ArrowBooleanValueAccess());
//		case DOUBLE:
//			return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
//				childAllocator, new ArrowDoubleColumnPartitionFactory(childAllocator, m_batchSize),
//				() -> new ArrowDoubleValueAccess());
//		case STRING:
//			return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.Utf8(), childAllocator,
//				new ArrowStringColumnPartitionFactory(childAllocator, m_batchSize), () -> new ArrowStringValueAccess());
//		default:
//			throw new UnsupportedOperationException("not yet implemented");
//	}
//}
}
