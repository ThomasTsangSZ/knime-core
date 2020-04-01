
package org.knime.core.data.store.arrow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.data.store.column.partition.ColumnPartition;
import org.knime.core.data.store.column.partition.ColumnPartitionWriter;

import io.netty.buffer.ArrowBuf;

/**
 * Persists the vectors of a column in sequential order.
 */
public final class ArrowVectorToDiskWriter<V extends FieldVector> implements ColumnPartitionWriter<V> {

	/* Initialized in constructor */
	private final File m_baseDir;
	private final BufferAllocator m_allocator;
	private final Schema m_schema;

	/* Lazily initialized */
	private ArrowFileWriter m_writer;
	private Path m_file;
	private VectorSchemaRoot m_root;

	public ArrowVectorToDiskWriter(final File baseDir, final Schema schema, final BufferAllocator allocator) {
		m_allocator = allocator;
		m_baseDir = baseDir;
		m_schema = schema;
	}

	@SuppressWarnings("resource")
	private void initWriter() throws IOException {
		if (m_writer == null) {
			m_file = Files.createTempFile(m_baseDir.toPath(), UUID.randomUUID().toString(), ".arrow");
			m_root = VectorSchemaRoot.create(m_schema, m_allocator);
			m_writer = new ArrowFileWriter(m_root, null, new RandomAccessFile(m_file.toFile(), "rw").getChannel());
		}
	}

	@Override
	public void write(final ColumnPartition<V> partition) throws IOException {
		initWriter();
		final List<ArrowFieldNode> nodes = new ArrayList<>();
		final List<ArrowBuf> buffers = new ArrayList<>();
		final V vector = partition.get();
		appendNodes(vector, nodes, buffers);
		try (final ArrowRecordBatch batch = new ArrowRecordBatch(partition.getNumValues(), nodes, buffers)) {
			m_writer.writeBatch();
		}
	}

	// TODO: Copied from org.apache.arrow.vector.VectorUnloader. Is there a better
	// way to do all of this (including writing vectors in general)?
	private void appendNodes(final FieldVector vector, final List<ArrowFieldNode> nodes, final List<ArrowBuf> buffers) {
		nodes.add(new ArrowFieldNode(vector.getValueCount(), vector.getNullCount()));
		final List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
		final int expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField().getType());
		if (fieldBuffers.size() != expectedBufferCount) {
			throw new IllegalArgumentException(
					String.format("wrong number of buffers for field %s in vector %s. found: %s", vector.getField(),
							vector.getClass().getSimpleName(), fieldBuffers));
		}
		buffers.addAll(fieldBuffers);
		for (final FieldVector child : vector.getChildrenFromFields()) {
			appendNodes(child, nodes, buffers);
		}
	}

	@Override
	public void close() throws Exception {
		// just close the writer. keep persisted data.

		// TODO not entirely sure in which order we have to close or if we have to close
		// all or... (later!)
		m_root.close();
		m_writer.close();
	}
}
