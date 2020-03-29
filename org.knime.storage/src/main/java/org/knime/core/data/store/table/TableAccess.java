package org.knime.core.data.store.table;

import org.knime.core.data.store.Access;
import org.knime.core.data.store.DataValue;

public interface TableAccess<V extends DataValue> extends Access<Row<V>> {
}
