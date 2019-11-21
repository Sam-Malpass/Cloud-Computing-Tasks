package courseworkTasks.experimental.task1Chain;

import courseworkTasks.preprocessFunctions.Preprocessor;
import mapReduce.Job;
import mapReduce.Tuple;
import java.util.ArrayList;

public class Task1A extends Job {

    static Preprocessor preprocessor = new Preprocessor();


    @Override
    public ArrayList<Object> preprocess(ArrayList<Object> arrayList) {
        ArrayList<Object> preprocessed = new ArrayList<>();
        for(Object o : arrayList) {
            Object tmp = preprocessor.preprocess(o);
            if(tmp != null){
                preprocessed.add(tmp);
            }
        }
        ArrayList<Object> unique = new ArrayList<>();
        unique.add(preprocessed.get(0));
        for(Object o : preprocessed){
            ArrayList<Object> tmp = (ArrayList) o;
            boolean flag = true;
            for(Object t : unique) {
                ArrayList<Object> tmp2 = (ArrayList) t;
                if(tmp2.get(1).equals(tmp.get(1)) && tmp2.get(2).equals(tmp.get(2))) {
                    flag = false;
                }
            }
            if(flag) {
                unique.add(tmp);
            }
        }
        return unique;
    }

    @Override
    public ArrayList<Tuple> map(ArrayList<Object> arrayList) {
        ArrayList<Tuple> mapperOutput = new ArrayList<>();
        for(Object o : arrayList) {
            ArrayList<Object> tmp = (ArrayList) o;
            Tuple tmpTuple = new Tuple(tmp.get(2), tmp.get(1));
            mapperOutput.add(tmpTuple);
        }
        return mapperOutput;
    }

    @Override
    public ArrayList<Tuple> reduce(ArrayList<Tuple> arrayList) {
        ArrayList<Tuple> reducerOutput = new ArrayList<>();
        for(Tuple t : arrayList) {
            ArrayList<Object> vals = (ArrayList) t.getValue();
            Tuple tmp = new Tuple(t.getKey(), vals.size());
            reducerOutput.add(tmp);
        }
        return reducerOutput;
    }

    @Override
    public String format(ArrayList<Tuple> arrayList) {
        String builder = "";
        for(Tuple t : arrayList) {
            builder = builder + "Airport ID: " + t.getKey() + "\nNumber of Flights: " + t.getValue() + "\n\n";
        }
        return builder;
    }

    public static Preprocessor getPreprocessor() {
        return preprocessor;
    }
}
