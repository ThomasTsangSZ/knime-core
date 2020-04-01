package org.knime.core.data.store.arrow.column;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.column.partition.AbstractColumnPartition;

public class ArrowColumnPartition<T extends FieldVector> extends AbstractColumnPartition<T> {

	public ArrowColumnPartition(T storage) {
		super(storage, storage.getValueCapacity(), storage.getValueCount());
	}

	@Override
	public void close() throws Exception {
		m_storage.close();
	}

}
