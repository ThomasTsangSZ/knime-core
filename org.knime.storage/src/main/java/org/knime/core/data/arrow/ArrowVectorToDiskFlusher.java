
package org.knime.core.data.arrow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.data.cache.SequentialCacheFlusher;
import org.knime.core.data.table.column.Partition;

import io.netty.buffer.ArrowBuf;

final class ArrowVectorToDiskFlusher<V extends FieldVector> implements SequentialCacheFlusher<V>, AutoCloseable {

	private final VectorSchemaRoot m_root;

	private final ArrowFileWriter m_writer;

	public ArrowVectorToDiskFlusher(final File file, final Schema schema, final BufferAllocator allocator)
			throws FileNotFoundException {
		m_root = VectorSchemaRoot.create(schema, allocator);
		// TODO: Figure out if closing the channel also closes the file.
		m_writer = new ArrowFileWriter(m_root, null, new RandomAccessFile(file, "rw").getChannel());
	}

	@Override
	public void flush(final Partition<V> vector) throws IOException {
		final List<ArrowFieldNode> nodes = new ArrayList<>();
		final List<ArrowBuf> buffers = new ArrayList<>();
		appendNodes(vector.get(), nodes, buffers);
		try (final ArrowRecordBatch batch = new ArrowRecordBatch(vector.get().getValueCount(), nodes, buffers)) {
			m_writer.writeBatch();
		}
	}

	// TODO: Copied from org.apache.arrow.vector.VectorUnloader. Is any better
	// way?
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
		m_writer.close();
		m_root.close();
	}
}
