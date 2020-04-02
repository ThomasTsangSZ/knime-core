
package org.knime.core.data.store.arrow;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.knime.core.data.store.arrow.therealstore.ArrowColumnPartitionStore;
import org.knime.core.data.store.arrow.therealstore.ArrowTableStore;
import org.knime.core.data.store.column.ColumnSchema;
import org.knime.core.data.store.column.ReadableColumn;
import org.knime.core.data.store.column.WritableColumnCursor;
import org.knime.core.data.store.column.partition.PartitionedReadableColumnCursor;
import org.knime.core.data.store.column.partition.PartitionedReadableValueAccess;
import org.knime.core.data.store.column.partition.PartitionedWritableColumnCursor;
import org.knime.core.data.store.table.ReadableTable;
import org.knime.core.data.store.table.WritableTable;

// TODO why does arrow need to know about tables at all? :-)
// TODO: This class really only does the plumbing (the remaining Arrow-specific
// stuff can easily be moved out).
public final class ArrowTable implements ReadableTable, WritableTable {

	private final ColumnSchema[] m_schema;

	private final ReadableColumn[] m_readableColumns;

	private final WritableColumnCursor[] m_writableColumnCursors;

	private final ArrowTableStore m_store;

	public ArrowTable(final ColumnSchema[] schema, final ArrowTableStore store) {
		m_schema = schema;
		m_store = store;
		m_readableColumns = new ReadableColumn[schema.length];
		m_writableColumnCursors = new WritableColumnCursor[schema.length];
	}

	@Override
	public long getNumColumns() {
		return m_schema.length;
	}

	@Override
	public ReadableColumn getReadableColumn(final long columnIndexLong) {
		final int columnIndex = Math.toIntExact(columnIndexLong);
		ReadableColumn readableColumn = m_readableColumns[columnIndex];
		if (readableColumn == null) {
			readableColumn = () -> {
				ArrowColumnPartitionStore<?> columnStore;
				try {
					columnStore = m_store.getColumnPartitionStore(Math.toIntExact(columnIndex));
				}
				catch (final IOException ex) {
					throw new UncheckedIOException(ex); // TODO
				}
				return new PartitionedReadableColumnCursor(createReadableAccess(columnIndex), columnStore.createReader());
			};
			m_readableColumns[columnIndex] = readableColumn;
		}
		return readableColumn;
	}

	@Override
	public WritableColumnCursor getWritableColumnCursor(final long columnIndexLong) {
		final int columnIndex = Math.toIntExact(columnIndexLong);
		final WritableColumnCursor writableColumnCursor = m_writableColumnCursors[columnIndex];
		if (writableColumnCursor == null) {
			ArrowColumnPartitionStore<?> columnStore;
			try {
				columnStore = m_store.getColumnPartitionStore(Math.toIntExact(columnIndex));
			}
			catch (final IOException ex) {
				throw new UncheckedIOException(ex); // TODO
			}
			m_writableColumnCursors[columnIndex] = new PartitionedWritableColumnCursor<>(access);
		}
		return writableColumnCursor;
	}

	private PartitionedReadableValueAccess<?> createReadableAccess(final int columnIndex) {
		throw new IllegalStateException("not yet implemented"); // TODO: implement

	}

//	private ArrowColumnAccess<? extends FieldVector> addColumn(final ColumnType type) {
//		final BufferAllocator childAllocator = m_rootAllocator.newChildAllocator("ChildAllocator", 0, m_rootAllocator
//			.getLimit());
//		switch (type) {
//			case BOOLEAN:
//				return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.Binary(), childAllocator,
//					new ArrowBooleanColumnPartitionFactory(childAllocator, m_batchSize), () -> new ArrowBooleanValueAccess());
//			case DOUBLE:
//				return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
//					childAllocator, new ArrowDoubleColumnPartitionFactory(childAllocator, m_batchSize),
//					() -> new ArrowDoubleValueAccess());
//			case STRING:
//				return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.Utf8(), childAllocator,
//					new ArrowStringColumnPartitionFactory(childAllocator, m_batchSize), () -> new ArrowStringValueAccess());
//			default:
//				throw new UnsupportedOperationException("not yet implemented");
//		}
//	}
}
