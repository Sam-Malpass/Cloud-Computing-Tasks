package courseworkTasks.experimental.task4;

import mapReduce.Job;
import mapReduce.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Task4B extends Job {
    @Override
    public ArrayList<Object> preprocess(ArrayList<Object> arrayList) {
        return arrayList;
    }

    @Override
    public ArrayList<Tuple> map(ArrayList<Object> arrayList) {
        ArrayList<Tuple> mapperOutput = new ArrayList<>();
        for(Object o : arrayList) {
            Tuple tmp = (Tuple) o;
            ArrayList<Object> oldKey = (ArrayList) tmp.getKey();
            ArrayList<Object> oldVals = (ArrayList) tmp.getValue();
            for(Object o2 : oldVals) {
                mapperOutput.add(new Tuple(o2, oldKey.get(1)));
            }
        }
        return mapperOutput;
    }

    @Override
    public ArrayList<Tuple> reduce(ArrayList<Tuple> arrayList) {
        ArrayList<Tuple> reducerOutput = new ArrayList<>();
        for(Tuple t : arrayList) {
            ArrayList<Object> tmp = (ArrayList) t.getValue();
            double totalMiles = 0;
            for(Object o : tmp) {
                totalMiles += (double) o;
            }
            reducerOutput.add(new Tuple(t.getKey(), totalMiles));
        }
        return reducerOutput;
    }

    @Override
    public String format(ArrayList<Tuple> arrayList) {
        String builder = "";
        for(Tuple t : arrayList) {
            builder += "Passenger " + t.getKey() + " has " + t.getValue() + " air miles\n";
        }
        Collections.sort(arrayList, new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                return Double.compare(((double)o1.getValue()), ((double)o2.getValue()));
            }
        });
        builder += "Passenger " + arrayList.get(arrayList.size()-1).getKey() + " had the highest air miles\n\n";
        return builder;
    }
}
