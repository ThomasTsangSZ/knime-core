package org.knime.core.data.store.table;

import org.knime.core.data.store.Bounded;
import org.knime.core.data.store.DataValue;

// TODO at some point we could distinguish between "bounded" and "unbounded" tables.
// TODO for now we default to "bounded"
public interface TableAccessibleBounded<V extends DataValue> extends TableAccessible<V>, Bounded {

}