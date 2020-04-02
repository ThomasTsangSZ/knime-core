
package org.knime.core.data.store.arrow.column;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.store.column.partition.AbstractColumnPartition;

public final class ArrowColumnPartition<T extends FieldVector> extends AbstractColumnPartition<T> {

	public ArrowColumnPartition(final T storageVector) {
		super(storageVector, storageVector.getValueCapacity(), storageVector.getValueCount());
	}

	@Override
	public void close() throws Exception {
		m_storage.close();
	}
}
