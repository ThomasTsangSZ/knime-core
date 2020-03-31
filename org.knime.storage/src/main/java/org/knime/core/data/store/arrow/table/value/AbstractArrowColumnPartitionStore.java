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

	protected int m_batchSize;

	public AbstractArrowColumnPartitionStore(BufferAllocator allocator, File baseDir, final int batchSize) {
		m_allocator = allocator;
		m_batchSize = batchSize;

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
		return new ArrowColumnPartition(vector, m_numPartitions - 1, m_batchSize);
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
				// all good, let's GO!
				return m_reader.read(m_idx++);
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
		private final long m_batchSize;
		private int m_numValues;

		ArrowColumnPartition(V vector, long partitionIdx, final long batchSize) {
			m_vector = vector;
			m_partitionIdx = partitionIdx;
			m_batchSize = batchSize;
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
		public int getCapacity() {
			return (int) m_batchSize;
		}

		@Override
		public long getPartitionIndex() {
			return m_partitionIdx;
		}

		@Override
		public int getNumValues() {
			return m_numValues;
		}

		@Override
		public void setNumValues(int numValues) {
			m_numValues = numValues;
		}

	}
}