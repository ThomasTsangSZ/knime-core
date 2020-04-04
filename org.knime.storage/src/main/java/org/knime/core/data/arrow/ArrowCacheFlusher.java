package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.knime.core.data.cache.SequentialCacheFlusher;
import org.knime.core.data.partition.Partition;

import io.netty.buffer.ArrowBuf;

public class ArrowCacheFlusher<F extends FieldVector> implements AutoCloseable, SequentialCacheFlusher<F> {

	private final File m_file;
	private ArrowStreamWriter m_writer;
	private VectorLoader m_vectorLoader;

	public ArrowCacheFlusher(final File file) throws IOException {
		m_file = file;
	}

	@SuppressWarnings("resource")
	@Override
	public void flush(Partition<F> partition) throws IOException {

		if (m_writer == null) {
			VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(partition.get().getField()),
					Collections.singletonList(partition.get()));
			m_vectorLoader = new VectorLoader(root);
			m_writer = new ArrowStreamWriter(root, null, new RandomAccessFile(m_file, "rw").getChannel());
		}

		// TODO there must be a better way?!
		final List<ArrowFieldNode> nodes = new ArrayList<>();
		final List<ArrowBuf> buffers = new ArrayList<>();
		appendNodes(partition.get(), nodes, buffers);

		// Auto-closing makes sure that ArrowRecordBatch actually releases the buffers
		// again
		try (final ArrowRecordBatch batch = new ArrowRecordBatch(partition.getNumValues(), nodes, buffers)) {
			m_vectorLoader.load(batch);
			m_writer.writeBatch();
		}
	}

	@Override
	public void close() throws Exception {
		if (m_writer != null) {
			m_writer.close();
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

}