package org.knime.core.data.store.chunk;
//
//package org.knime.core.data.store.vec;
//
//import java.util.Iterator;
//
//import org.knime.core.data.store.vec.rw.ReadableVectorAccess;
//import org.knime.core.data.store.vec.rw.WritableVectorAccess;
//
//public class VecAccessibleOnVecAccessibles implements VecAccessible {
//
//	private final Iterable<VecAccessible> m_accessibles;
//
//	private WritableVectorAccess m_writeAccess;
//
//	public VecAccessibleOnVecAccessibles(final Iterable<VecAccessible> accessibles) {
//		m_accessibles = accessibles;
//	}
//
//	@Override
//	public WritableVectorAccess getWriteAccess() {
//		// TODO: Make sure that only one write access is active per accessible at a
//		// time
//		if (m_writeAccess == null) {
//			final Iterator<VecAccessible> it = m_accessibles.iterator();
//			m_writeAccess = new WritableVectorAccess() {
//
//				private WritableVectorAccess m_curr = it.next().getWriteAccess();
//
//				@Override
//				public void fwd() {
//					if (!m_curr.canFwd()) { // TODO: Re-introduce canFwd for writables?
//						try {
//							m_curr.close();
//							m_curr = it.next().getWriteAccess();
//						}
//						catch (final Exception e) {
//							throw new RuntimeException(e);
//						}
//					}
//					m_curr.fwd();
//				}
//
//				@Override
//				public void setMissing() {
//					m_curr.setMissing();
//				}
//
//				// TODO: Special primitive accesses cannot be supported like this
//				// (setDoubleValue etc.)
//
//				@Override
//				public void close() throws Exception {
//					m_curr.close();
//				}
//			};
//		}
//		return m_writeAccess;
//	}
//
//	@Override
//	public ReadableVectorAccess createReadAccess() {
//		final Iterator<VecAccessible> it = m_accessibles.iterator();
//		return new ReadableVectorAccess() {
//
//			private ReadableVectorAccess m_curr = it.next().createReadAccess();
//
//			@Override
//			public boolean canFwd() {
//				return m_curr.canFwd() || it.hasNext();
//			}
//
//			@Override
//			public void fwd() {
//				if (!m_curr.canFwd()) {
//					try {
//						m_curr.close();
//						m_curr = it.next().createReadAccess();
//					}
//					catch (final Exception e) {
//						throw new RuntimeException(e);
//					}
//				}
//				m_curr.fwd();
//			}
//
//			@Override
//			public boolean isMissing() {
//				return m_curr.isMissing();
//			}
//
//			// TODO: Special primitive accesses cannot be supported like this
//			// (getDoubleValue etc.)
//
//			@Override
//			public void close() throws Exception {
//				m_curr.close();
//			}
//		};
//	}
//
//	@Override
//	public void close() throws Exception {
//		for (final VecAccessible accessible : m_accessibles) {
//			accessible.close();
//		}
//	}
//}
