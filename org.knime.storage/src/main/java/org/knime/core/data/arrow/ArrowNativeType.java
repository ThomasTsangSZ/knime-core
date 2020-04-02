package org.knime.core.data.arrow;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.data.vector.Vector;
import org.knime.core.data.vector.cache.SequentialCacheFlusher;
import org.knime.core.data.vector.cache.SequentialCacheLoader;
import org.knime.core.data.vector.table.VectorValue;

public interface ArrowNativeType<F extends FieldVector> {
	public Vector<F> createVector();
	public VectorValue<F> createVectorValue();

	public SequentialCacheLoader<Vector<F>> createLoader();

	public SequentialCacheFlusher<Vector<F>> createFlusher();
}
