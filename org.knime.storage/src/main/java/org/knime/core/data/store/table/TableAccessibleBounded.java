package org.knime.core.data.store.table;

import org.knime.core.data.store.Bounded;
import org.knime.core.data.store.Value;

// TODO at some point we could distinguish between "bounded" and "unbounded" tables.
// TODO for now we default to "bounded"
public interface TableAccessibleBounded<V extends Value> extends TableAccessible<V>, Bounded {

}