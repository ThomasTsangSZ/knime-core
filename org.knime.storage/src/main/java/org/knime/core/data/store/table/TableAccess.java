package org.knime.core.data.store.table;

import org.knime.core.data.store.Access;
import org.knime.core.data.store.Value;

public interface TableAccess<V extends Value> extends Access<Row<V>> {
}
