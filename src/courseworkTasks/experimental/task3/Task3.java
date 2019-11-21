package courseworkTasks.experimental.task3;

import courseworkTasks.preprocessFunctions.Preprocessor;
import mapReduce.Job;
import mapReduce.Tuple;

import java.util.ArrayList;

public class Task3 extends Job {
    Preprocessor preprocessor = new Preprocessor();
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
                if(tmp2.get(1).equals(tmp.get(1)) && tmp2.get(0).equals(tmp.get(0))) {
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
            ArrayList tmp = (ArrayList) o;
            Tuple tuple = new Tuple(tmp.get(1), tmp.get(0));
            mapperOutput.add(tuple);
        }
        return mapperOutput;
    }

    @Override
    public ArrayList<Tuple> reduce(ArrayList<Tuple> arrayList) {
        ArrayList<Tuple> reducerOutput = new ArrayList<>();
        for(Tuple t : arrayList) {
            Tuple tuple = new Tuple(t.getKey(), ((ArrayList) t.getValue()).size());
            reducerOutput.add(tuple);
        }
        return reducerOutput;
    }

    @Override
    public String format(ArrayList<Tuple> arrayList) {
        String builder = "";
        for(Tuple t : arrayList) {
            builder += "Flight " + t.getKey() + " had " + t.getValue() + " passenger(s)\n";
        }
        return builder;
    }
}
