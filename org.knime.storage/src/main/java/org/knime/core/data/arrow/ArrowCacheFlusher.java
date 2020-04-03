package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.knime.core.data.cache.SequentialCacheFlusher;
import org.knime.core.data.table.column.Partition;

import io.netty.buffer.ArrowBuf;

public class ArrowCacheFlusher<F extends FieldVector> implements AutoCloseable, SequentialCacheFlusher<F> {

	private ArrowFileWriter m_writer;
	private File m_file;

	public ArrowCacheFlusher(final File file) throws IOException {
		m_file = file;
	}

	@Override
	public void flush(Partition<F> partition) throws IOException {
		// TODO let's check later how expensive this is...
		final VectorSchemaRoot root = new VectorSchemaRoot(partition.get());
		if (m_writer == null) {
			m_writer = new ArrowFileWriter(root, null, new RandomAccessFile(m_file, "rw").getChannel());
		}

		// TODO there must be a better way?!
		final List<ArrowFieldNode> nodes = new ArrayList<>();
		final List<ArrowBuf> buffers = new ArrayList<>();
		appendNodes(partition.get(), nodes, buffers);

		// Auto-closing makes sure that ArrowRecordBatch actually releases the buffers
		// again
		try (final ArrowRecordBatch batch = new ArrowRecordBatch(partition.getNumValues(), nodes, buffers)) {
			m_writer.writeBatch();
		}

		// TODO OK here? releases fieldvectors...
		root.close();
	}

	@Override
	public void close() throws Exception {
		m_writer.close();
		// TODO what else do we have to close here?
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

}