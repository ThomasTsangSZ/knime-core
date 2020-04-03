package org.knime.core.data.arrow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.knime.core.data.cache.SequentialCacheFlusher;
import org.knime.core.data.table.column.Partition;

import io.netty.buffer.ArrowBuf;

public class ArrowCacheFlusher<F extends FieldVector> implements AutoCloseable, SequentialCacheFlusher<F> {

	private final Path m_baseDir;
	private final String m_id;
	private int m_partitionCtr;

	public ArrowCacheFlusher(final Path baseDir, final String id) throws IOException {
		m_baseDir = baseDir;
		m_id = id;
	}

	@SuppressWarnings("resource")
	@Override
	public void flush(Partition<F> partition) throws IOException {

		// TODO let's check later how expensive this is...
		// one file per partition. Assumption is that files are written sequentially
		final File file = new File(m_baseDir.toFile(), m_id + "_" + "" + m_partitionCtr++ + ".knarrow");
		file.deleteOnExit();
		try (VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(partition.get().getField()),
				Collections.singletonList(partition.get()));
				ArrowWriter writer = new ArrowFileWriter(root, null, new RandomAccessFile(file, "rw").getChannel())) {

			// TODO there must be a better way?!
			final List<ArrowFieldNode> nodes = new ArrayList<>();
			final List<ArrowBuf> buffers = new ArrayList<>();
			appendNodes(partition.get(), nodes, buffers);

			// Auto-closing makes sure that ArrowRecordBatch actually releases the buffers
			// again
			try (final ArrowRecordBatch batch = new ArrowRecordBatch(partition.getNumValues(), nodes, buffers)) {
				writer.writeBatch();
			}
		}
	}

	@Override
	public void close() throws Exception {
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