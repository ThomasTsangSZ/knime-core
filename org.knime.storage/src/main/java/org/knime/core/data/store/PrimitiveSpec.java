package org.knime.core.data.store;

public interface PrimitiveSpec {

	int getNumColumns();

	PrimitiveType getTypeAt(int i);

}
