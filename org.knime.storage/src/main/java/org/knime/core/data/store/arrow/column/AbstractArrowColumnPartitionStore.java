package org.knime.core.data.store.arrow.column;

import java.io.File;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.arrow.ArrowVectorFromDiskReader;
import org.knime.core.data.store.arrow.ArrowVectorToDiskWriter;
import org.knime.core.data.store.column.partition.AbstractColumnPartitionStore;
import org.knime.core.data.store.column.partition.ColumnPartition;
import org.knime.core.data.store.column.partition.DefaultColumnPartition;

// TODO Composition vs. inheritance?
// TODO Most of this is arrow independent code.
abstract class AbstractArrowColumnPartitionStore<V extends FieldVector> extends AbstractColumnPartitionStore<V> {

	private long m_numPartitions = 0;

	protected int m_batchSize;

	private BufferAllocator m_allocator;

	public AbstractArrowColumnPartitionStore(BufferAllocator allocator, File baseDir, final int batchSize) {
		super(new ArrowVectorToDiskWriter<V>(baseDir, null, allocator),
				new ArrowVectorFromDiskReader<V>(baseDir, null, allocator), batchSize);
		m_allocator = allocator;
	}

	// TODO not entirely sure who is responsible for closing the allocator... we
	// didn't create it so...
	@Override
	public void close() throws Exception {
		super.close();
		m_allocator.close();
	}

	// TODO more fine-granular synchronization possible?
	@Override
	public synchronized ColumnPartition<V> appendPartition() {
		return new DefaultColumnPartition<>(create(m_allocator), m_numPartitions++, m_batchSize);
	}

	// create a new chunk of data
	abstract V create(BufferAllocator allocator);

}