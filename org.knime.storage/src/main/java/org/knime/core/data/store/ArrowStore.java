
package org.knime.core.data.store;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.knime.core.data.store.table.column.ColumnSchema;
import org.knime.core.data.store.table.column.ReadableColumn;
import org.knime.core.data.store.table.column.WritableColumn;

public class ArrowStore implements Store {

	// numValues corresponds to numSchemata * BATCH_SIZE. not bytes!
	// TODO maybe adaptive later?
	private final int BATCH_SIZE = 1024;

	private final ColumnSchema[] m_schemas;

	private final WritableColumn[] m_writableLogicalColumns;

	private final ReadableColumn[] m_readableLogicalColumns;

	private final ValueVector[] m_columnVectors;

	private final RootAllocator m_rootAllocator;

	// TODO Schema should be implemented differently in case of very large tables
	// (
	// > INT.MAX_VALUE)
	// TODO e.g. different access on schemas & different column creation
	public ArrowStore(final ColumnSchema[] schemas) {
		m_schemas = schemas;

		// TODO likely we want to have a child-root allocator, passed from some
		// central
		// mem-management
		m_rootAllocator = new RootAllocator();

		m_columnVectors = new ValueVector[m_schemas.length];

		// TODO factory
		m_writableLogicalColumns = new WritableColumn[m_schemas.length];

		for (int i = 0; i < m_schemas.length; i++) {
			switch (m_schemas[i].getType()) {
				case DOUBLE: {
					final Float8Vector vector = new Float8Vector("TODO", m_rootAllocator);
					vector.allocateNew(BATCH_SIZE);
					m_columnVectors[i] = vector;
					m_writableLogicalColumns[i] = new ArrowWritableDoubleColumn(vector);
				}
				case STRING:
				default:
			}
		}

		m_readableLogicalColumns = new ReadableColumn[m_schemas.length];
		for (int i = 0; i < m_schemas.length; i++) {
			switch (m_schemas[i].getType()) {
				case DOUBLE: {
					m_readableLogicalColumns[i] = new ArrowReadableDoubleColumn((Float8Vector) m_columnVectors[i]);
				}
				case STRING:
				default:
			}
		}

	}

	@Override
	public long getNumLogicalColumns() {
		return m_schemas.length;
	}

	@Override
	public WritableColumn getWritableLogicalColumnAt(final long index) {
		return m_writableLogicalColumns[(int) index];
	}

	@Override
	public ReadableColumn getReadableLogicalColumnAt(final long index) {
		return m_readableLogicalColumns[(int) index];
	}
}
