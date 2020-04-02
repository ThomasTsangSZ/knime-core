
package org.knime.core.data.arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.table.column.NativeType;

import io.netty.buffer.ArrowBuf;

public final class ArrowUtils {

	private ArrowUtils() {
	}

	public static <F extends FieldVector> ArrowNativeType<F> create(NativeType type) {
		switch (type) {
		case BOOLEAN:
			break;
		case DOUBLE:
			break;
		case STRING:
			break;
		default:
			break;
		}
		throw new IllegalArgumentException();
	}

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
