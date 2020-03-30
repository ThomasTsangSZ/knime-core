package org.knime.core.data.store;

import org.apache.arrow.memory.RootAllocator;
import org.knime.core.data.store.table.column.ColumnSchema;
import org.knime.core.data.store.table.column.WritableColumn;

public class ArrowStore implements Store {

	// numValues corresponds to numSchemata * BATCH_SIZE. not bytes!
	// TODO maybe adaptive later?
	private final int BATCH_SIZE = 1024;

	private ColumnSchema[] m_schemas;

	private WritableColumn[] m_writableLogicalColumns;

	private RootAllocator m_rootAllocator;

	// TODO Schema should be implemented differently in case of very large tables (
	// > INT.MAX_VALUE)
	// TODO e.g. different access on schemas & different column creation
	public ArrowStore(ColumnSchema[] schemas) {
		m_schemas = schemas;

		// TODO likely we want to have a child-root allocator, passed from some central
		// mem-management
		m_rootAllocator = new RootAllocator();
		
		// TODO factory
		m_writableLogicalColumns = new WritableColumn[m_schemas.length];

		for (int i = 0; i < m_schemas.length; i++) {
			switch (m_schemas[i].getType()) {
			case DOUBLE:
				m_writableLogicalColumns[i] = new ArrowWritableDoubleColumn(m_rootAllocator, BATCH_SIZE);
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
	public WritableColumn getLogicalColumnAt(long index) {
		return m_writableLogicalColumns[(int) index];
	}

	@Override
	public void closeForWriting() {

	}

}
