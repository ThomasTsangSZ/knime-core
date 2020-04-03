package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.data.cache.SequentialCacheLoader;
import org.knime.core.data.table.column.Partition;

/* NB: This reader has best performance when data is accessed sequentially row-wise.
* TODO Maybe different flush / loader combinations are configurable per node later?
*/
public class ArrowCacheLoader<V extends FieldVector> implements AutoCloseable, SequentialCacheLoader<V> {

	// some constants
	private final File m_file;
	private final BufferAllocator m_alloc;

	// Lazy initialization of readers
	private ArrowFileReader m_reader;
	private List<ArrowBlock> m_recordBlocks;

	// Varies with each partition
	private VectorSchemaRoot m_root;

	// TODO support for column filtering and row filtering ('TableFilter'), i.e.
	// only load required columns / rows from disc. Rows should be easily possible
	// by using 'ArrowBlock'
	// TODO maybe easier with parquet backend?
	public ArrowCacheLoader(final File file, BufferAllocator alloc, Field field, int batchSize) throws IOException {
		m_alloc = alloc;
		m_file = file;
		m_root = VectorSchemaRoot.create(new Schema(Collections.singleton(field)), alloc);
	}

	@Override
	public Partition<V> load(long index) throws IOException {
		// create new reader if needed
		if (m_reader == null) {
			m_reader = new ArrowFileReader(new RandomAccessFile(m_file, "rw").getChannel(), m_alloc);
			m_recordBlocks = m_reader.getRecordBlocks();
		} else {
			// only need to realloc after first iteration
			m_root.allocateNew();
		}

		// Only load each partition once. If user jumps between partitions... own fault!
		m_reader.loadRecordBatch(m_recordBlocks.get((int) index));
		// load next batch
		m_reader.loadNextBatch();

		@SuppressWarnings("unchecked")
		final V vector = (V) m_root.getVector(0);

		// TODO is ref counting here like that correct?
		ArrowUtils.retainVector(vector);
		final ArrowPartition<V> partition = new ArrowPartition<>(vector);
		partition.setNumValues(vector.getValueCount());
		return partition;
	}

	@Override
	public void close() throws Exception {
		m_root.close();
		m_reader.close();

		// TODO can we close alloc here?
		m_alloc.close();
	}

}
