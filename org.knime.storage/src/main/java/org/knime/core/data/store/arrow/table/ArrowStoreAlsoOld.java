//
//package org.knime.core.data.store.arrow.table;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//
//import org.apache.arrow.memory.RootAllocator;
//import org.knime.core.data.store.VecAccessible;
//import org.knime.core.data.store.chunk.ChunkStore;
//import org.knime.core.data.store.table.column.ColumnSchema;
//
//// TODO maybe we want to actually split into read/write later.
//// TODO: Only stores chunks of single columns after the most recent changes
//public class ArrowStoreAlsoOld implements ChunkStore<VecAccessible> {
//
//	private final ArrowVecFactory m_factory;
//
//	private final ColumnSchema m_spec;
//
//	private final File m_dest;
//
//	private final RootAllocator m_root;
//
//	private int m_numChunks;
//
//	public ArrowStoreAlsoOld(final File dest, final ColumnSchema spec, final long limit) {
//		// TODO add allocation listener for this store.
//		// TODO likely we need one central allocator for ALL tables/stores
//		m_root = new RootAllocator(limit);
//
//		// TODO where is the batch-size coming from?
//		m_factory = new ArrowVecFactory(2048, m_root);
//		m_spec = spec;
//		m_dest = dest;
//	}
//
//	@Override
//	public ColumnSchema schema() {
//		return m_spec;
//	}
//
//	@Override
//	public void persist(final VecAccessible batch) {
//		// TODO write to disc.
//		// TODO one file per batch. maybe physical batch size != logical (CPU cache
//		// etc)
//
//		// NB: this method can also be called during reading (i.e. when a chunk has
//		// been cached before).
//	}
//
//	@Override
//	public VecAccessible load(final long idx) {
//		// TODO load from disc
//		return null;
//	}
//
//	@Override
//	public VecAccessible createNext() {
//		// TODO in the KNIME case this method will never be called after load(long
//		// idx) has been called.
//		// TODO however in general - we can create new batches while reading - this
//		// is possible... undecided what to do. use-case: streaming.
//
//		m_numChunks++;
//		// batc to be closed by caller
//		return m_factory.create(m_spec.getType());
//	}
//
//	@Override
//	public void destroy() {
//		// Assumption: we never, ever keep any references on batches, so all
//		// references
//		// should have been closed externally.
//		// however as a fallback we could simply kill the root allocator for this
//		// store.
//		// TODO check if this is correct, in case someone is still "accessing the
//		// batch"
////		m_root.close();
//		try {
//			Files.delete(m_dest.toPath());
//		}
//		catch (final IOException e) {
//			// TODO
//			throw new RuntimeException(e);
//		}
//	}
//
//	@Override
//	public long numChunks() {
//		return m_numChunks;
//	}
//}
