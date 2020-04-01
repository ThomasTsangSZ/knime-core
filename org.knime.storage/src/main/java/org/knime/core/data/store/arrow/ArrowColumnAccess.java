package org.knime.core.data.store.arrow;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.data.store.arrow.column.ArrowColumnPartition;
import org.knime.core.data.store.column.access.ColumnAccess;
import org.knime.core.data.store.column.partition.ColumnPartition;
import org.knime.core.data.store.column.partition.ColumnPartitionFactory;
import org.knime.core.data.store.column.partition.ColumnPartitionReader;
import org.knime.core.data.store.column.partition.ColumnPartitionValueAccess;

import io.netty.buffer.ArrowBuf;

class ArrowColumnAccess<T extends FieldVector> implements ColumnAccess<T> {

	private ColumnPartitionFactory<T> m_factory;
	private Supplier<ColumnPartitionValueAccess<T>> m_linkedType;
	private Schema m_schema;
	private BufferAllocator m_allocator;
	private File m_file;
	private ArrowColumnAccess<T>.ArrowVectorToDiskWriter m_writer;

	public ArrowColumnAccess(final File baseDir, final ArrowType type, final BufferAllocator allocator,
			ColumnPartitionFactory<T> factory, Supplier<ColumnPartitionValueAccess<T>> linkedType) {

		m_schema = new Schema(Collections.singleton(new Field("TODO", new FieldType(true, type, null), null)));
		m_allocator = allocator;
		m_file = new File(baseDir, UUID.randomUUID().toString() + ".arrow");
		m_writer = new ArrowVectorToDiskWriter();

		m_factory = factory;
		m_linkedType = linkedType;
	}

	@Override
	public void close() throws Exception {
		m_writer.close();
	}

	@Override
	public ColumnPartitionReader<T> create() {
		return new ArrowVectorFromDiscReader();
	}

	@Override
	public void write(ColumnPartition<T> partition) throws IOException {
		m_writer.write(partition);
	}

	@Override
	public ColumnPartition<T> appendPartition() {
		return m_factory.appendPartition();
	}

	@Override
	public ColumnPartitionValueAccess<T> createLinkedType() {
		return m_linkedType.get();
	}

	@Override
	public void destroy() throws Exception {
		close();
		m_file.delete();
	}

	@Override
	public long getNumPartitions() {
		// TODO Auto-generated method stub
		return 0;
	}

	// TODO we assume read after write with this implementation.
	// TODO this might not be the case when a reader starts reading from a cache
	// while the cache is still persistent. then memory event. cache empty. reader
	// has to start reading.
	class ArrowVectorFromDiscReader implements ColumnPartitionReader<T> {

		private VectorSchemaRoot m_root;
		private ArrowFileReader m_reader;
		private List<ArrowBlock> m_recordBlocks;

		private int m_currendRecord;

		@Override
		public void close() throws Exception {
			m_root.close();
			m_reader.close();
		}

		@SuppressWarnings("resource")
		@Override
		public boolean hasNext() {
			// TODO I've the strong suspicion that his is really uncool to do this in
			// 'hasNext'
			try {
				if (m_reader == null) {
					m_reader = new ArrowFileReader(new RandomAccessFile(m_file, "rw").getChannel(), m_allocator);
					m_root = VectorSchemaRoot.create(m_schema, m_allocator);
					m_recordBlocks = m_reader.getRecordBlocks();
					m_reader.loadNextBatch();
				}
				return m_currendRecord < m_recordBlocks.size();
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		}

		@Override
		public ColumnPartition<T> next() {
			try {
				m_reader.loadRecordBatch(m_recordBlocks.get(m_currendRecord));
				@SuppressWarnings("unchecked")
				final T vector = (T) m_root.getVector(0);
				ArrowUtils.retainVector(vector);

				// no idea if this is beneficial or not.
				if (hasNext()) {
					m_root.allocateNew();
				}
				return new ArrowColumnPartition<T>(vector);
			} catch (IOException e) {
				// TODO
				throw new RuntimeException(e);
			}
		}

		@Override
		public void skip() {
			m_currendRecord++;
		}
	}

	class ArrowVectorToDiskWriter {

		/* Lazily initialized */
		private ArrowFileWriter m_writer;
		private File m_file;
		private VectorSchemaRoot m_root;

		@SuppressWarnings("resource")
		private void initWriter() throws IOException {
			if (m_writer == null) {
				m_root = VectorSchemaRoot.create(m_schema, m_allocator);
				m_writer = new ArrowFileWriter(m_root, null, new RandomAccessFile(m_file, "rw").getChannel());
			}
		}

		private void write(final ColumnPartition<T> partition) throws IOException {
			initWriter();
			final List<ArrowFieldNode> nodes = new ArrayList<>();
			final List<ArrowBuf> buffers = new ArrayList<>();
			final T vector = partition.get();
			appendNodes(vector, nodes, buffers);
			// Auto-closing makes sure that ArrowRecordBatch actually releases the buffers
			// again
			try (final ArrowRecordBatch batch = new ArrowRecordBatch(partition.getNumValues(), nodes, buffers)) {
				m_writer.writeBatch();
			}
		}

		// TODO: Copied from org.apache.arrow.vector.VectorUnloader. Is there a better
		// way to do all of this (including writing vectors in general)?
		private void appendNodes(final FieldVector vector, final List<ArrowFieldNode> nodes,
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

		void close() throws Exception {
			// just close the writer. keep persisted data.
			// TODO not entirely sure in which order we have to close or if we have to close
			// all or... (later!)
			m_writer.close();
			m_root.close();
		}
	}

}
