package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Collections;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.data.cache.SequentialCacheLoader;
import org.knime.core.data.table.column.Partition;

/* NB: This reader has best performance when data is accessed sequentially row-wise.
* TODO Maybe different flush / loader combinations are configurable per node later?
*/
public class ArrowCacheLoader<V extends FieldVector> implements AutoCloseable, SequentialCacheLoader<V> {

	// some constants
	private final BufferAllocator m_alloc;

	// Varies with each partition
	private VectorSchemaRoot m_root;

	private Path m_baseDir;

	private String m_id;

	// TODO support for column filtering and row filtering ('TableFilter'), i.e.
	// only load required columns / rows from disc. Rows should be easily possible
	// by using 'ArrowBlock'
	// TODO maybe easier with parquet backend?
	public ArrowCacheLoader(final Path baseDir, String id, BufferAllocator alloc) throws IOException {
		m_alloc = alloc;
		m_baseDir = baseDir;
		m_id = id;
	}

	@SuppressWarnings("resource")
	@Override
	public Partition<V> load(long index) throws IOException {
		// create new reader if needed
		final File file = new File(m_baseDir.toFile(), m_id + "_" + index + ".knarrow");
		try (ArrowFileReader reader = new ArrowFileReader(new RandomAccessFile(file, "rw").getChannel(), m_alloc)) {
			// load next batch
			reader.loadNextBatch();

			@SuppressWarnings("unchecked")
			final V vector = (V) reader.getVectorSchemaRoot().getVector(0);

			// TODO is ref counting here like that correct?
			ArrowUtils.retainVector(vector);
			final ArrowPartition<V> partition = new ArrowPartition<>(vector);
			partition.setNumValues(vector.getValueCount());
			return partition;

			// TODO arrow closes file.
		}

	}

	@Override
	public void close() throws Exception {
		m_root.close();

		// TODO can we close alloc here?
		m_alloc.close();
	}

}
