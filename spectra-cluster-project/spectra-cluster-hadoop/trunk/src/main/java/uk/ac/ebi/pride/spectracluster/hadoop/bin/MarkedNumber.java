package uk.ac.ebi.pride.spectracluster.hadoop.bin;

/**
 * Associate a double - usually >= 0 with an object
 * most obvious use if for APrioriBinning to assign big jobs to early reduce tasks and
 * sheild these from later additions
 *
 * @author Steve Lewis
 * @author Rui Wang
 */
public class MarkedNumber<T> implements Comparable<MarkedNumber<T>> {

    private final T mark;
    private final double value;

    public MarkedNumber(final T pMark, final double pValue) {
        mark = pMark;
        value = pValue;
    }

    public T getMark() {
        return mark;
    }

    public double getValue() {
        return value;
    }

    /**
     * sort my value highest first then mark
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(final MarkedNumber<T> o) {
        double myValue = getValue();
        double oValue = o.getValue();
        if (myValue != oValue)
            return myValue > oValue ? -1 : 1;
        T me = getMark();
        if (me instanceof Comparable)
            return ((Comparable<T>) me).compareTo(o.getMark());
        return me.toString().compareTo(o.getMark().toString());

    }

    public String toString() {
        return String.format("%s:%6.3f", getMark(), getValue());
    }
}
