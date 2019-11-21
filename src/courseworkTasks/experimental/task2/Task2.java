package courseworkTasks.experimental.task2;

import courseworkTasks.preprocessFunctions.Preprocessor;
import mapReduce.Job;
import mapReduce.Tuple;

import java.util.ArrayList;
import java.util.Date;

public class Task2 extends Job {

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
            ArrayList<Object> tmp = (ArrayList) o;
            ArrayList<Object> key = new ArrayList<>();
            key.add(tmp.get(1));
            key.add(tmp.get(2));
            key.add(tmp.get(4));
            key.add(tmp.get(3));
            key.add( new Date(((Date)tmp.get(4)).getTime() + (((int)tmp.get(5)) * 60)*1000));
            key.add(tmp.get(5));
            Tuple tuple = new Tuple(key, tmp.get(0));
            mapperOutput.add(tuple);
        }
        return mapperOutput;
    }

    @Override
    public ArrayList<Tuple> reduce(ArrayList<Tuple> arrayList) {
        return arrayList;
    }

    @Override
    public String format(ArrayList<Tuple> arrayList) {
        String builder = "";
        for(Tuple t : arrayList) {
            ArrayList key = (ArrayList) t.getKey();
            builder += "Flight " + key.get(0) + "\nDeparted from " + key.get(1) + " at " + key.get(2) + "\nArrived at " + key.get(3) + " at " + key.get(4) + "\nTotal time of " + key.get(5) + " minutes. \nPassenger manifest:\n";
            ArrayList val = (ArrayList) t.getValue();
            for(Object o : val) {
                builder += o.toString() + "\n";
            }
            builder += "\n\n";
        }
        return builder;
    }
}
