package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.knime.core.data.cache.RefCountingPartition;
import org.knime.core.data.cache.SequentialCacheLoader;

/* NB: This reader has best performance when data is accessed sequentially row-wise.
* TODO Maybe different flush / loader combinations are configurable per node later?
*/
public class ArrowCacheLoader<V extends FieldVector> implements AutoCloseable, SequentialCacheLoader<V> {

	// some constants
	private final BufferAllocator m_alloc;

	// Varies with each partition
	private VectorSchemaRoot m_root;

	private File m_file;

	private ArrowStreamReader m_reader;

	private VectorUnloader m_unloader;

	// TODO support for column filtering and row filtering ('TableFilter'), i.e.
	// only load required columns / rows from disc. Rows should be easily possible
	// by using 'ArrowBlock'
	// TODO maybe easier with parquet backend?
	public ArrowCacheLoader(final File file, final BufferAllocator alloc) throws IOException {
		m_alloc = alloc;
		m_file = file;
	}

	// Assumption for this reader: sequential loading.
	@SuppressWarnings("resource")
	@Override
	public RefCountingPartition<V> load(long index) throws IOException {
		if (m_reader == null) {
			m_reader = new ArrowStreamReader(new RandomAccessFile(m_file, "rw").getChannel(), m_alloc);
			m_root = m_reader.getVectorSchemaRoot();
			m_unloader = new VectorUnloader(m_root);
		}

		// load next
		m_reader.loadNextBatch();

		// Transfer buffers to new vector. Zero copy.
		// TODO Too expensive?
		final VectorSchemaRoot root = VectorSchemaRoot.create(m_root.getSchema(), m_alloc);
		final VectorLoader loader = new VectorLoader(root);
		loader.load(m_unloader.getRecordBatch());

		@SuppressWarnings("unchecked")
		final V vector = (V) root.getVector(0);
		final ArrowPartition<V> partition = new ArrowPartition<>(vector, index);
		partition.setNumValues(vector.getValueCount());

		return partition;
	}

	@Override
	public void close() throws Exception {
		if (m_root != null) {
			m_root.close();
			m_reader.close();
			m_alloc.close();
		}
	}
}
