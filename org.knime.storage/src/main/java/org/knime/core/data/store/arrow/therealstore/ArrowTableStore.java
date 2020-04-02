
package org.knime.core.data.store.arrow.therealstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Flushable;
import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

// TODO: Yeah I know, the class contains "Table" in its name. But it's really
// only there to have a central place that holds the allocator and creates the
// vectors and their files in a common base directory ;-).
public final class ArrowTableStore implements Flushable, AutoCloseable {

	private final File m_baseDirectory;

	private final Schema[] m_schema;

	private final BufferAllocator m_allocator;

	private final ArrowColumnPartitionStore<?>[] m_vectorStores;

	public ArrowTableStore(final File baseDirectory, final Schema[] schema, final BufferAllocator allocator)
		throws FileNotFoundException
	{
		if (!baseDirectory.exists()) {
			throw new FileNotFoundException("Base directory does not exist: " + baseDirectory.toString());
		}
		m_baseDirectory = baseDirectory;
		m_schema = schema;
		m_allocator = allocator;
		m_vectorStores = new ArrowColumnPartitionStore<?>[schema.length];
	}

	public ArrowColumnPartitionStore<?> getColumnPartitionStore(final int index) throws IOException {
		ArrowColumnPartitionStore<?> vectorStore = m_vectorStores[index];
		if (vectorStore == null) {
			final File vectorFile = new File(m_baseDirectory, Integer.toString(index));
			vectorFile.createNewFile();
			vectorStore = new ArrowColumnPartitionStore<>(vectorFile, m_schema[index], m_allocator);
			m_vectorStores[index] = vectorStore;
		}
		return vectorStore;
	}

	@Override
	public void flush() throws IOException {
		for (final ArrowColumnPartitionStore<?> vectorStore : m_vectorStores) {
			vectorStore.flush();
		}
	}

	// TODO i'm wouldn't know what 'close()' means for this table
	// 'close()' -> release memory
	// 'destroy()' delete any trace of this table
	// NB: We don't need 'closeForWriting()'. Design allows to have concurrent
	// read/write (e.g. for streaming)
	@Override
	public void close() throws Exception {
		for (final ArrowColumnPartitionStore<?> vectorStore : m_vectorStores) {
			vectorStore.close();
		}
	}

	public void destroy() throws Exception {
		for (final ArrowColumnPartitionStore<?> vectorStore : m_vectorStores) {
			vectorStore.destroy();
		}
	}
}
