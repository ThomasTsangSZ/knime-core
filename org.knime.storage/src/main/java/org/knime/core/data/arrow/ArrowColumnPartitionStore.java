
package org.knime.core.data.arrow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.data.vector.ColumnPartitionReader;
import org.knime.core.data.vector.ColumnPartitionWriter;
import org.knime.core.data.vector.Vector;
import org.knime.core.data.vector.VectorGroup;
import org.knime.core.data.vector.cache.SequentialCache;
import org.knime.core.data.vector.cache.SequentialCacheFlusher;

public final class ArrowColumnPartitionStore<P extends FieldVector> implements VectorGroup<P> {

	private final File m_file;

	private final Schema m_schema;

	private final BufferAllocator m_allocator;

	// TODO: Type on some Cacheable<V> that does the reference counting?!
	private final SequentialCache<P> m_cache;

	public ArrowColumnPartitionStore(final File file, final Schema schema, final BufferAllocator allocator)
		throws FileNotFoundException
	{
		m_file = file;
		m_schema = schema;
		m_allocator = allocator;
		final SequentialCacheFlusher<P> flusher = new ArrowVectorToDiskFlusher<>(file, schema, allocator);
		m_cache = new SequentialCache<>(flusher);
	}

	@Override
	public ColumnPartitionReader<P> createReader() {
		return new ColumnPartitionReader<P>() {

			private final ArrowVectorFromDiskLoader<P> m_loader = createLoader();

			private long m_index = -1;

			@Override
			public boolean hasNext() {
				throw new IllegalStateException("not yet implemented"); // TODO
			}

			@Override
			public Vector<P> next() {
				final P partition = m_cache.get(++m_index, m_loader);
				throw new IllegalStateException("not yet implemented");
			}

			@Override
			public void close() throws Exception {
				throw new IllegalStateException("not yet implemented"); // TODO
			}
		};
	}

	private ArrowVectorFromDiskLoader<P> createLoader() {
		try {
			return new ArrowVectorFromDiskLoader<>(m_file, m_schema, m_allocator);
		}
		catch (final FileNotFoundException ex) {
			// Should not happen. Otherwise this class's constructor would already
			// have thrown.
			throw new IllegalStateException(ex);
		}
	}

	@Override
	public ColumnPartitionWriter<P> getWriter() {
		return new ColumnPartitionWriter<P>() {

			@Override
			public void write(final Vector<P> partition) throws IOException {
				m_cache.add(partition.get());
			}

			@Override
			public void close() throws Exception {
				throw new IllegalStateException("not yet implemented"); // TODO
			}
		};
	}

	@Override
	public void flush() throws IOException {
		m_cache.flush();
	}

	@Override
	public void close() throws Exception {
		m_cache.close();
	}

	@Override
	public void destroy() throws Exception {
		// TODO
	}
}
