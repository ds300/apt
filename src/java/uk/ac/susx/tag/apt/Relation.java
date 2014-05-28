package uk.ac.susx.tag.apt;

/**
 * A Relation represents a typed, directed relationship between two entities
 * @author ds300
 */
public class Relation {
    public final int dependent;
    public final int governor;
    public final int type;

    public Relation (int dependent, int governor, int type) {
        if (type == 0) throw new RuntimeException("Relation type indices cannot be zero");
        this.dependent = dependent;
        this.governor = governor;
        this.type = type;
    }
}
