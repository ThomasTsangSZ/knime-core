
package org.knime.core.data.store.arrow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import io.netty.buffer.ArrowBuf;

/**
 * Persists the vectors of a column in sequential order.
 */
public final class ArrowVectorToDiskWriter<T extends FieldVector> implements AutoCloseable {

	private final File m_file;

	private final VectorLoader m_loader;

	private final ArrowFileWriter m_writer;

	public ArrowVectorToDiskWriter(final File file, final Schema vectorSchema, final BufferAllocator allocator) {
		m_file = file;
		final VectorSchemaRoot root = VectorSchemaRoot.create(vectorSchema, allocator);
		m_loader = new VectorLoader(root);
		// TODO: Make sure closing the channel also closes the file.
		try {
			m_writer = new ArrowFileWriter(root, null, new RandomAccessFile(m_file, "rw").getChannel());
		} catch (FileNotFoundException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	public void write(final T vector) throws IOException {
		final List<ArrowFieldNode> nodes = new ArrayList<>();
		final List<ArrowBuf> buffers = new ArrayList<>();
		appendNodes(vector, nodes, buffers);
		try (final ArrowRecordBatch batch = new ArrowRecordBatch(vector.getValueCount(), nodes, buffers)) {
			m_loader.load(batch);
			m_writer.writeBatch();
		}
		// TODO: Make sure buffers are released everywhere.
	}

	// TODO: Copied from org.apache.arrow.vector.VectorUnloader. Is there a better
	// way to do all of this (including writing vectors in general)?
	private static void appendNodes(final FieldVector vector, final List<ArrowFieldNode> nodes,
			final List<ArrowBuf> buffers) {
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
		m_writer.close();
	}

	public void deleteWrittenData() throws IOException {
		Files.delete(m_file.toPath());
	}
}
