package uk.ac.susx.tag.apt;

import it.unimi.dsi.fastutil.ints.Int2FloatSortedMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
import pl.edu.icm.jlargearrays.ByteLargeArray;

/**
 * Interfaces specifying the core operations of Anchored Packed Trees (APTs)
 * @author ds300
 */
public interface APT {
    /**
     * Get the child node of this APT matching the specified relation, or null if no such child exists.
     * @param relation the ID of the relation
     * @return the child node, or null if not found
     */
    APT getChild(int relation);


    /**
     * Get the entity score for this node
     * @param entityId the ID of the token to retrieve the score for
     * @return the score
     */
    float getScore(int entityId);

    /**
     * Get the sum of all scores at this node
     * @return
     */
    float sum();

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
     * @return a map from entity IDs to scores for this node
     */
    Int2FloatSortedMap entityScores();

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
     * as {@link APT#walk(uk.ac.susx.tag.apt.APTVisitor)}, but only goes {@code depth} nodes deep.
     * @param visitor
     * @param depth
     */
    void walk(APTVisitor visitor, int depth);

    /**
     * Creates a new APT which includes the specified token/score pair. Replacing any existing score for
     * {@code token}.
     * @param tokenId the ID of the token
     * @param score the score
     * @return the new APT
     */
    APT withScore(int tokenId, float score);

    /**
     * Creates a new APT which includes the specified token/score pair. Incrementing any existing score for
     * {@code token} by {@code score}.
     * @param tokenId the ID of the token
     * @param score the score
     * @return the new APT
     */
    APT withIncrementedScore(int tokenId, float score);

    /**
     * Creates a new APT which includes the specified edge. The child node will be copied with a reverse edge inserted.
     * @param dependencyId the dependency ID labelling the edge
     * @param child the child node
     * @return the new APT
     */
    APT withEdge(int dependencyId, APT child);

    /**
     * Serializes the APT to byte format. The format is as follows (integers in big-endian)
     *    int: number of bytes to store entity-score pairs
     *    int: number of bytes to store children
     *    int: number of children
     *    (int int)*: entity-score pairs stored serially in ascending order of key
     *                e.g. the map {0: 1, 3: 4} would be 0x00000000 00000001 00000003 00000004
     *    (int APT)*: relation-child paris stored serially in ascending order of relation id, where APT is the format
     *                described here.
     *
     * Byte arrays are used for (de)serialization rather than output streams for the sake of minimizing allocations,
     * especially during serialization.
     *
     * @return the serialized byte array.
     */
    byte[] toByteArray();

    /**
     * TODO: Write Documentation
     */
    ByteLargeArray toLargeByteArray();
}

