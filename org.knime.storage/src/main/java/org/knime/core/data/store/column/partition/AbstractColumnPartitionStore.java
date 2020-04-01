package org.knime.core.data.store.column.partition;

import java.io.IOException;

// TODO Composition vs. inheritance?
public abstract class AbstractColumnPartitionStore<T> implements ColumnPartitionStore<T> {

	protected ColumnPartitionWriter<T> m_writer;
	protected ColumnPartitionReader<T> m_reader;
	protected long m_partitionCtr = 0;

	protected int m_batchSize;

	public AbstractColumnPartitionStore(ColumnPartitionWriter<T> writer, ColumnPartitionReader<T> reader,
			int batchSize) {
		m_writer = writer;
		m_reader = reader;
		m_batchSize = batchSize;
	}

	@Override
	public long getNumPartitions() {
		return m_partitionCtr;
	}

	@Override
	public void close() throws Exception {
		// release all memory associated with this allocator.
		m_reader.close();
		m_writer.close();
	}

	@Override
	public void persist(ColumnPartition<T> partition) throws IOException {
		m_writer.write(partition);
	}

	// TODO java_doc!
	@Override
	public ColumnPartitionIterator<T> iterator() {
		return new ColumnPartitionIterator<T>() {

			@Override
			public boolean hasNext() {
				return m_reader.hasNext();
			}

			@Override
			public ColumnPartition<T> next() {
				// all good, let's GO!
				return m_reader.readNext();
			}

			@Override
			public void skip() {
				m_reader.skip();
			}
		};
	}

}