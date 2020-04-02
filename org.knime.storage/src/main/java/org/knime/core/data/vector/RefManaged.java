package org.knime.core.data.vector;

// TODO For thread-safety we might need a reference manager (MW)
public interface RefManaged {
	void release();

	void retain();
}
