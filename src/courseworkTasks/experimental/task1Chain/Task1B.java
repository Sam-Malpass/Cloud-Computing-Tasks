package courseworkTasks.experimental.task1Chain;

import courseworkTasks.experimental.task1Chain.Task1A;
import courseworkTasks.preprocessFunctions.Preprocessor;
import mapReduce.Job;
import mapReduce.Tuple;
import java.util.ArrayList;

public class Task1B extends Job {

    Preprocessor preprocessor = Task1A.getPreprocessor();

    @Override
    public ArrayList<Object> preprocess(ArrayList<Object> arrayList) {
        ArrayList<Object> allAiports = new ArrayList<>();
        for(Object o : arrayList)
        {
            Tuple tmp = (Tuple) o;
            allAiports.add(new Tuple(tmp.getKey(), true));
        }
        for(Object o : preprocessor.getAirportCodes())
        {
            boolean shouldAdd = true;
            for(Object t : allAiports)
            {
                Tuple tmp = (Tuple) t;
                if(((Tuple) t).getKey().equals(o))
                {
                    shouldAdd = false;
                }
            }
            if(shouldAdd)
            {
                allAiports.add(new Tuple(o, false));
            }
        }
        return allAiports;
    }

    @Override
    public ArrayList<Tuple> map(ArrayList<Object> arrayList) {
        ArrayList<Tuple> mapperOutputs = new ArrayList<>();
        for(Object o : arrayList)
        {
            Tuple tmp = (Tuple) o;
            if((boolean)tmp.getValue() == false)
            {
                mapperOutputs.add(tmp);
            }
        }
        return mapperOutputs;
    }

    @Override
    public ArrayList<Tuple> reduce(ArrayList<Tuple> arrayList) {
        return arrayList;
    }

    @Override
    public String format(ArrayList<Tuple> arrayList) {
        String builder = "Missing Airports:\n";
        for(Tuple t : arrayList)
        {
            builder += t.getKey() + "\n";
        }
        return builder;
    }
}
