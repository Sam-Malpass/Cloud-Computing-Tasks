/**
 * Levensthein
 * @author Sam Malpass
 */
package courseworkTasks.preprocessFunctions;

public class Levensthein {
    public Levensthein() {}

    private int getLargest(String x, String y) {
        if(x.length() > y.length()) {
            return x.length();
        }
        else if(y.length() > x.length()) {
            return y.length();
        }
        else {
            return x.length();
        }
    }

    public int getDistance(String x, String y) {
        int numMatches = 0;
        for(Character c1 : x.toCharArray()) {
            for(Character c2 : y.toCharArray()) {
                if(c1.equals(c2)) {
                    numMatches++;
                }
            }
        }
        return getLargest(x,y) - numMatches;
    }
}
