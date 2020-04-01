package org.knime.core.data.store.arrow;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.knime.core.data.store.arrow.column.ArrowBooleanColumnPartitionFactory;
import org.knime.core.data.store.arrow.column.ArrowBooleanColumnPartitionFactory.ArrowBooleanValueAccess;
import org.knime.core.data.store.arrow.column.ArrowDoubleColumnPartitionFactory;
import org.knime.core.data.store.arrow.column.ArrowDoubleColumnPartitionFactory.ArrowDoubleValueAccess;
import org.knime.core.data.store.arrow.column.ArrowStringColumnPartitionFactory;
import org.knime.core.data.store.arrow.column.ArrowStringColumnPartitionFactory.ArrowStringValueAccess;
import org.knime.core.data.store.column.ColumnSchema;
import org.knime.core.data.store.column.ColumnType;
import org.knime.core.data.store.column.ReadableColumn;
import org.knime.core.data.store.column.ReadableColumnCursor;
import org.knime.core.data.store.column.WritableColumnCursor;
import org.knime.core.data.store.column.partition.PartitionedReadableColumnCursor;
import org.knime.core.data.store.column.partition.PartitionedWritableColumn;
import org.knime.core.data.store.table.ReadableTable;
import org.knime.core.data.store.table.WritableTable;

// TODO many things could be factored out.
// TODO we could factor out the store etc to be more flexible (later)
// TODO why does arrow need to know about tables at all? :-)
public class ArrowTable implements ReadableTable, WritableTable {

	private final ArrowTableStore m_store;
	private final List<CachedColumnAccess<? extends FieldVector>> m_columnAccesses = new ArrayList<>();

	public ArrowTable(File baseDir, final ColumnSchema[] schema, int batchSize) throws IOException {
		m_store = new ArrowTableStore(baseDir, schema, batchSize);
	}

	@Override
	public ReadableColumn getReadableColumn(long columnIndex) {
		return new ReadableColumn() {
			@Override
			public ReadableColumnCursor createCursor() {
				// NB: Stupid Java
				return createReadableColumnCursor(m_store.getColumnStoreAt((int) columnIndex));
			}
		};
	}

	@Override
	public WritableColumnCursor getWritableColumnCursor(long columnIndex) {
		final ArrowColumnAccess<? extends FieldVector> access = m_store.getColumnStoreAt((int) columnIndex);
		return createWritableColumn(access);
	}

	private <T extends FieldVector> WritableColumnCursor createWritableColumn(ArrowColumnAccess<T> access) {
		return new PartitionedWritableColumn<>(access, access.createLinkedType());
	}

	private <T extends FieldVector> ReadableColumnCursor createReadableColumnCursor(ArrowColumnAccess<T> columnStore) {
		return new PartitionedReadableColumnCursor<>(columnStore.create(), columnStore.createLinkedType());
	}

	@Override
	public long getNumColumns() {
		return m_store.getNumColumns();
	}

	// TODO i'm wouldn't know what 'close()' means for this table
	// 'close()' -> release memory
	// 'destroy()' delete any trace of this table
	// NB: We don't need 'closeForWriting()'. Design allows to have concurrent
	// read/write (e.g. for streaming)
	@Override
	public void close() throws Exception {
		m_store.close();

	}

	public void flush() throws IOException {
		for (final CachedColumnAccess<?> cached : m_columnAccesses) {
			cached.flush();
		}
	}

	class ArrowTableStore {

		private final File m_baseDir;
		private final RootAllocator m_rootAllocator;
		private ColumnSchema[] m_schema;
		private int m_batchSize;

		public ArrowTableStore(File baseDir, final ColumnSchema[] schema, int batchSize) {
			m_rootAllocator = new RootAllocator();
			m_baseDir = baseDir;
			m_schema = schema;
			m_batchSize = batchSize;

			for (int i = 0; i < m_schema.length; i++) {
				m_columnAccesses.add(new CachedColumnAccess<>(addColumn(m_schema[i].getType())));
			}
		}

		private ArrowColumnAccess<? extends FieldVector> addColumn(ColumnType type) {
			final BufferAllocator childAllocator = m_rootAllocator.newChildAllocator("ChildAllocator", 0,
					m_rootAllocator.getLimit());
			switch (type) {
			case BOOLEAN:
				return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.Binary(), childAllocator,
						new ArrowBooleanColumnPartitionFactory(childAllocator, m_batchSize),
						() -> new ArrowBooleanValueAccess());
			case DOUBLE:
				return new DefaultArrowColumnAccess<>(m_baseDir,
						new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), childAllocator,
						new ArrowDoubleColumnPartitionFactory(childAllocator, m_batchSize),
						() -> new ArrowDoubleValueAccess());
			case STRING:
				return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.Utf8(), childAllocator,
						new ArrowStringColumnPartitionFactory(childAllocator, m_batchSize),
						() -> new ArrowStringValueAccess());
			default:
				throw new UnsupportedOperationException("not yet implemented");
			}
		}

		// TODO sanity checking etc
		public ArrowColumnAccess<? extends FieldVector> getColumnStoreAt(int idx) {
			return m_columnAccesses.get(idx);
		}

		// TODO Interface?
		public void close() throws Exception {
			for (ArrowColumnAccess<?> store : m_columnAccesses) {
				store.close();
			}
		}

		public void destroy() throws Exception {
			for (final ArrowColumnAccess<?> columnStore : m_columnAccesses) {
				columnStore.close();
				columnStore.destroy();
			}
		}

		public long getNumColumns() {
			return m_schema.length;
		}
	}

}
