package uk.ac.susx.tag.apt;

/**
 * @author ds300
 */
public interface APTVisitor<T extends APT> {
    void visit(int[] path, T APT);
}
