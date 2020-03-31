package org.knime.core.data.store.arrow.table.value;

import java.io.File;
import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.arrow.table.ArrowVectorFromDiskReader;
import org.knime.core.data.store.arrow.table.ArrowVectorToDiskWriter;
import org.knime.core.data.store.partition.ColumnPartition;
import org.knime.core.data.store.partition.ColumnPartitionIterator;
import org.knime.core.data.store.partition.ColumnPartitionStore;

abstract class AbstractArrowColumnPartitionStore<V extends FieldVector> implements ColumnPartitionStore<V> {

	private final BufferAllocator m_allocator;
	private long m_numPartitions = 0;
	private ArrowVectorToDiskWriter<V> m_writer;
	private ArrowVectorFromDiskReader<V> m_reader;

	public AbstractArrowColumnPartitionStore(BufferAllocator allocator, File baseDir) {
		m_allocator = allocator;

		// TODO
//		m_writer = new ArrowVectorToDiskWriter<V>(baseDir, null, allocator);
//		m_reader = new ArrowVectorFromDiskReader<V>(baseDir, null, allocator);

	}

	@Override
	public long getNumPartitions() {
		return m_numPartitions;
	}

	@Override
	public void close() throws Exception {
		// release all memory associated with this allocator.
		m_allocator.close();
	}

	@Override
	public void persist(ColumnPartition<V> partition) throws IOException {
		m_writer.write(partition.get());
	}

	// TODO more fine-granular synchronization possible?
	@Override
	public synchronized ColumnPartition<V> appendPartition() {
		// Create
		final V vector = create(m_allocator);
		m_numPartitions++;
		return new ArrowColumnPartition(vector, m_numPartitions - 1);
	}

	// TODO java_doc!
	@Override
	public ColumnPartitionIterator<V> iterator() {
		return new ColumnPartitionIterator<V>() {

			private long m_idx = 0;

			@Override
			public boolean hasNext() {
				// could be dynamic :-)
				// TODO in case of streaming (i.e. writing while reading or vice-versa) we could
				// add a flag here.
				return m_idx < m_numPartitions;
			}

			@Override
			public ColumnPartition<V> next() {
				m_idx++;
				return m_reader.read(m_idx);
			}

			@Override
			public void skip() {
				m_idx++;
			}
		};
	}

	// create a new chunk of data
	abstract V create(BufferAllocator allocator);

	class ArrowColumnPartition implements ColumnPartition<V> {

		private final V m_vector;
		private final long m_partitionIdx;

		ArrowColumnPartition(V vector, long partitionIdx) {
			m_vector = vector;
			m_partitionIdx = partitionIdx;
		}

		@Override
		public void close() throws Exception {
			m_vector.close();
		}

		@Override
		public V get() {
			return m_vector;
		}

		@Override
		public int getValueCount() {
			return m_vector.getValueCount();
		}

		@Override
		public int getValueCapacity() {
			return m_vector.getValueCapacity();
		}

		@Override
		public long getPartitionIndex() {
			return m_partitionIdx;
		}

	}
}