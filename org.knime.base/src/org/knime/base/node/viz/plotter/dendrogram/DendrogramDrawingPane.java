/*
 * ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright, 2003 - 2009
 * University of Konstanz, Germany
 * Chair for Bioinformatics and Information Mining (Prof. M. Berthold)
 * and KNIME GmbH, Konstanz, Germany
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.org
 * email: contact@knime.org
 * -------------------------------------------------------------------
 * 
 * History
 *   10.10.2006 (Fabian Dill): created
 */
package org.knime.base.node.viz.plotter.dendrogram;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.Stroke;
import java.awt.event.MouseEvent;
import java.util.List;

import org.knime.base.node.viz.plotter.AbstractDrawingPane;
import org.knime.base.node.viz.plotter.LabelPaintUtil;
import org.knime.core.data.RowKey;
import org.knime.core.data.property.ColorAttr;
import org.knime.core.data.property.ShapeFactory;

/**
 * Interprets the {@link org.knime.base.node.viz.plotter.dendrogram.BinaryTree}
 * of {@link org.knime.base.node.viz.plotter.dendrogram.DendrogramPoint}s such
 * that the leaf nodes (the data points) are painted and the cluster nodes are
 * drawn with a horizontal line between the contained subnodes and vertical 
 * lines to that subnodes. The distance of the two subclsuters is displayed on
 * the y axis, the data points are displayed on the x axis. A tooltip to provide
 * information about the exct distance is provided for the nodes.
 * 
 * @author Fabian Dill, University of Konstanz
 */
public class DendrogramDrawingPane extends AbstractDrawingPane {
    
    /** The tree containing the DendrogramPoints. */
    private BinaryTree<DendrogramPoint> m_rootNode;
    
    /** Flag to display or hide the points. */
    private boolean m_showDots = true;
    
    private int m_dotSize = 4;
    
    private int m_lineThickness = 1;
    
    /** Constant by which the thickness of hilited or selected lines is 
     * increased. */
    private static final float EMPH = 1.4f;
    
    /**
     * Inititalizes the tooltip.
     *
     */
    public DendrogramDrawingPane() {
        setToolTipText("");
    }
    
    /**
     * Sets the view model (a binary tree containing points).
     * @param root the view model.
     */
    public void setRootNode(final BinaryTree<DendrogramPoint> root) {
        m_rootNode = root;
    }
    
    /**
     * 
     * @param show true if dots should be displayed
     */
    public void setShowDots(final boolean show) {
        m_showDots = show;
    }
    
    /**
     * 
     * @return the size of the dots.
     */
    public int getDotSize() {
        return m_dotSize;
    }
    
    /**
     * 
     * @param dotSize sets the dot size
     */
    public void setDotSize(final int dotSize) {
        m_dotSize = dotSize;
    }
    
    /**
     * 
     * @param thickness sets the line thickness
     */
    public void setLineThickness(final int thickness) {
        m_lineThickness = thickness;
    }
    

    /**
     * Paints a dendrogram such that the leaf nodes are painted at the border
     * (with distance 0) and on the nominal x axis and the cluster nodes are 
     * drawn with increasing distance to the top (that is the distance is 
     * plotted on the y axis). Each cluster node has exactly two subnodes, 
     * a horizontal line on the height of the distance between these two 
     * subnodes and two vertical lines from the end points of the 
     * horizontal line to the subnodes. 
     * 
     * <p>
     * Hilited (sub)nodes are colored with the hilite color and hilited and 
     * selected clusteres are visualized through color and emphasized line 
     * thickness.
     * 
     * {@inheritDoc}
     */
    @Override
    public void paintContent(final Graphics g) {
        if (m_rootNode == null) {
            return;
        }
        Stroke backupStroke = ((Graphics2D)g).getStroke();
        Color backupColor = g.getColor();
        List<BinaryTreeNode<DendrogramPoint>>nodes = m_rootNode.getNodes(
                BinaryTree.Traversal.IN);
        for (BinaryTreeNode<DendrogramPoint> node : nodes) {
            DendrogramPoint dendroPoint = node.getContent();
            // set the correct stroke and color
            g.setColor(ColorAttr.DEFAULT.getColor(
                    node.getContent().isSelected(), 
                    node.getContent().isHilite()));
            if (node.getContent().isSelected() 
                    || node.getContent().isHilite()) {
                ((Graphics2D)g).setStroke(new BasicStroke(
                        (float)(m_lineThickness * EMPH)));
            } else {
                ((Graphics2D)g).setStroke(new BasicStroke(m_lineThickness));
            }
            
            if (node.getLeftChild() != null 
                    || node.getRightChild() != null) {
//                draw horizontal line
                Point leftPoint = node.getLeftChild().getContent().getPoint();
                Point rightPoint = node.getRightChild().getContent().getPoint();
                g.drawLine(leftPoint.x, node.getContent().getPoint().y, 
                        rightPoint.x, node.getContent().getPoint().y);
            }
            // draw vertical line
            if (node.getParent() != null) {
                g.setColor(ColorAttr.DEFAULT.getColor(
                        node.getParent().getContent().isSelected(), 
                        node.getParent().getContent().isHilite()));
                // check if parent is selected
                // if yes bold line, else normal line
                if (!node.getParent().getContent().isSelected() 
                        && !node.getParent().getContent().isHilite()) {
                    ((Graphics2D)g).setStroke(new BasicStroke(m_lineThickness));
                } else {
                    ((Graphics2D)g).setStroke(new BasicStroke(
                            (float)(m_lineThickness * EMPH)));
                }
                g.drawLine(node.getContent().getPoint().x, 
                        node.getContent().getPoint().y,
                        node.getContent().getPoint().x, 
                        node.getParent().getContent().getPoint().y);
            }
            if (m_showDots) {
                Point p = node.getContent().getPoint();
                ShapeFactory.Shape shape = node.getContent().getShape();
                int size = (int)(node.getContent().getRelativeSize() 
                        * m_dotSize);
                shape.paint(g, p.x, p.y, size, 
                        node.getContent().getColor().getColor(), 
                        dendroPoint.isHilite(), dendroPoint.isSelected(), 
                        false);
            }
            ((Graphics2D)g).setStroke(backupStroke);
            g.setColor(backupColor);
        }
    }
    
    
    /**
     * The original (not mapped) distance of the clustering between the two
     * subnodes. 
     * 
     * {@inheritDoc}
     */
    @Override
    public String getToolTipText(final MouseEvent event) {
        if (m_rootNode == null) {
            return "";
        }
        Point p = event.getPoint();
        List<BinaryTreeNode<DendrogramPoint>> nodes = m_rootNode.getNodes(
                BinaryTree.Traversal.IN);
        for (BinaryTreeNode<DendrogramPoint> node : nodes) {
            Point nodePoint = node.getContent().getPoint();
            double dotSize = m_dotSize 
                    * node.getContent().getRelativeSize();
            if (p.x > nodePoint.x - (dotSize / 2) 
                    && p.x < nodePoint.x + (dotSize / 2)
                    && p.y > nodePoint.y - (dotSize / 2)
                    && p.y < nodePoint.y + (dotSize / 2)) {
                if (node.getContent().getRows().size() == 1) {
                    for (RowKey row : node.getContent().getRows()) {
                        return row.toString();
                    }
                } else {
                    return "dist: " + LabelPaintUtil.getDoubleAsString(
                            node.getContent().getDistance(), 1000);
                }
            }
                    
        }
        return "";
    }

}
