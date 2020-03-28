package org.knime.core.data.store;

public interface BatchColumnAccessibleFactory {
	BatchColumnAccessible create(BatchColumnType type);
}
