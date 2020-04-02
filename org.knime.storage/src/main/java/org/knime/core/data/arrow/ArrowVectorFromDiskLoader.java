
package org.knime.core.data.arrow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.data.cache.SequentialCacheLoader;
import org.knime.core.data.table.column.Partition;

public final class ArrowVectorFromDiskLoader<V extends FieldVector> implements SequentialCacheLoader<V>, AutoCloseable {

	private final VectorSchemaRoot m_root;

	private final ArrowFileReader m_reader;

	public ArrowVectorFromDiskLoader(final File file, final Schema schema, final BufferAllocator allocator)
			throws FileNotFoundException {
		m_root = VectorSchemaRoot.create(schema, allocator);
		// TODO: Figure out if closing the channel also closes the file.
		m_reader = new ArrowFileReader(new RandomAccessFile(file, "rw").getChannel(), allocator);
	}

	@Override
	public Partition<V> load(final long index) throws IOException {
		// Random access should be doable via
		// ArrowFileReader#loadRecordBatch(ArrowBlock block).
		throw new IllegalStateException("not yet implemented"); // TODO: implement
	}

	@Override
	public void close() throws Exception {
		m_reader.close();
		m_root.close();
	}
}
