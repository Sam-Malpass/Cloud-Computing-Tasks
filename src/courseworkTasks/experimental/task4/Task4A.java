package courseworkTasks.experimental.task4;

import courseworkTasks.preprocessFunctions.Preprocessor;
import mapReduce.Job;
import mapReduce.Tuple;

import javax.sound.midi.SysexMessage;
import java.util.ArrayList;

public class Task4A extends Job {

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
            ArrayList<Object> key = new ArrayList<>();
            key.add(tmp.get(1));
            ArrayList<Tuple> airports = preprocessor.getAirports();
            for(Tuple t : airports) {
                ArrayList<Object> tmpCoords = (ArrayList) t.getValue();
                if(t.getKey().equals(tmp.get(2))) {
                    double startLat = (double) tmpCoords.get(1);
                    double startLong = (double) tmpCoords.get(2);
                    for(Tuple t2 : airports) {
                        if(t2.getKey().equals(tmp.get(3))) {
                            tmpCoords = (ArrayList) t2.getValue();
                            double finLat = (double) tmpCoords.get(1);
                            double finLong = (double) tmpCoords.get(2);
                            double theta = startLong - finLong;
                            double dist = Math.sin(Math.toRadians(startLat)) * Math.sin(Math.toRadians(finLat)) + Math.cos(Math.toRadians(startLat)) * Math.cos(Math.toRadians(finLat)) * Math.cos(Math.toRadians(theta));
                            dist = Math.acos(dist);
                            dist = Math.toDegrees(dist);
                            dist = dist * 60 * 1.1515;
                            key.add(dist);
                            mapperOutput.add(new Tuple(key, tmp.get(0)));
                        }
                    }
                }
            }
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
            ArrayList tmp = (ArrayList) t.getKey();
            builder += "Flight " + tmp.get(0) + " travelled " + tmp.get(1) + " nautical miles\n";
        }
        builder += "\n\n";
        return builder;
    }
}
