package uk.ac.susx.tag.apt;

import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;

/**
 * Interfaces specifying the core operations of Anchored Packed Trees (APTs)
 * @author ds300
 */
public interface APT extends Writable {
    /**
     * Get the child node of this APT matching the specified relation, or null if no such child exists.
     * @param relation the ID of the relation
     * @return the child node, or null if not found
     */
    APT getChild(int relation);


    /**
     * Get the entity count for this node
     * @param entityId the ID of the token to retrieve the count for
     * @return the count
     */
    int getCount(int entityId);

    /**
     * Get the sum of all counts at this node
     * @return
     */
    int sum();

    /**
     * Create a new APT which is the result of merging this pdt with {@code other} up to {@code depth} nodes deep.
     * @param other the other APT to merge
     * @param depth the maximum depth at which to merge
     * @return
     */
    APT merged(APT other, int depth);

    /**
     * Create a new APT which is the result of culling any nodes more than {@code depth} levels from this one.
     * @param depth The depth at which to cull
     * @return
     */
    APT culled(int depth);


    /**
     * @return a map from entity IDs to counts for this node
     */
    Int2IntSortedMap entityCounts();

    /**
     * @return a map from relation IDs to child nodes
     */
    Int2ObjectSortedMap<APT> edges();


    /**
     * Walks the APT, calling {@code visitor.visit()} at each node
     * @param visitor
     */
    void walk(APTVisitor visitor);

    /**
     * Creates a new APT which includes the specified token/count pair. Incrementing any existing count for
     * {@code token} by {@code count}.
     * @param tokenId the ID of the token
     * @param count the count
     * @return the new APT
     */
    APT withCount(int tokenId, int count);

    /**
     * Creates a new APT which includes the specified edge. The child node will be copied with a reverse edge inserted.
     * @param dependencyId the dependency ID labelling the edge
     * @param child the child node
     * @return the new APT
     */
    APT withEdge(int dependencyId, APT child);

}

