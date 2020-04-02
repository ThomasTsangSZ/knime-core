
package org.knime.core.data.arrow;

import com.google.common.io.Files;

import io.netty.buffer.ArrowBuf;

import java.io.File;
import java.io.IOException;

import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.table.column.ColumnSchema;
import org.knime.core.data.vector.table.VectorStoreBackedTable;

public final class ArrowUtils {

	private ArrowUtils() {}

	public static void retainVector(final ValueVector vector) {
		for (final ArrowBuf buffer : vector.getBuffers(false)) {
			buffer.getReferenceManager().retain();
		}
	}

	public static void releaseVector(final ValueVector vector) {
		for (final ArrowBuf buffer : vector.getBuffers(false)) {
			buffer.getReferenceManager().release();
		}
	}

	public static VectorStoreBackedTable createArrowTable(final int batchSize, final long offHeapSize, final ColumnSchema... schemas)
		throws IOException
	{
		final File baseDirectory = Files.createTempDir();
		baseDirectory.deleteOnExit();
		final ArrowTableStore store = new ArrowTableStore(baseDirectory, schemas, batchSize); // TODO
		return new VectorStoreBackedTable(schemas, store);
	}
	
//	private ArrowColumnAccess<? extends FieldVector> addColumn(final ColumnType type) {
//	final BufferAllocator childAllocator = m_rootAllocator.newChildAllocator("ChildAllocator", 0, m_rootAllocator
//		.getLimit());
//	switch (type) {
//		case BOOLEAN:
//			return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.Binary(), childAllocator,
//				new ArrowBooleanColumnPartitionFactory(childAllocator, m_batchSize), () -> new ArrowBooleanValueAccess());
//		case DOUBLE:
//			return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
//				childAllocator, new ArrowDoubleColumnPartitionFactory(childAllocator, m_batchSize),
//				() -> new ArrowDoubleValueAccess());
//		case STRING:
//			return new DefaultArrowColumnAccess<>(m_baseDir, new ArrowType.Utf8(), childAllocator,
//				new ArrowStringColumnPartitionFactory(childAllocator, m_batchSize), () -> new ArrowStringValueAccess());
//		default:
//			throw new UnsupportedOperationException("not yet implemented");
//	}
//}
}
