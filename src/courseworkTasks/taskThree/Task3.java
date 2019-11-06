package courseworkTasks.taskThree;

import mapReduce.Job;
import mapReduce.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class Task3 extends Job {
    @Override
    public ArrayList<Object> preprocess(ArrayList<Object> input) {
        ArrayList<Object> dataEntries = new ArrayList<>();
        for(Object y : input) {
            String line = (String) y;
            ArrayList<Object> data = new ArrayList<>();
            boolean errorFlag = false;
            String error = "";
            String[] row = line.split(",");
            ArrayList<String> rowList = new ArrayList(Arrays.asList(row));

            if(rowList.get(0).matches("[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][A-Z][A-Z][0-9]")) {
                data.add(rowList.get(0));
            }
            else {
                errorFlag = true;
                error = error + "Passenger ID\n";
            }

            if(rowList.get(1).matches("[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][A-Z]")) {
                data.add(rowList.get(1));
            }
            else {
                errorFlag = true;
                error = error + "Flight ID\n";
            }

            if(rowList.get(2).matches("[A-Z][A-Z][A-Z]")) {
                data.add(rowList.get(2));
            }
            else {
                errorFlag = true;
                error = error + "Departure Code\n";
            }

            if(rowList.get(3).matches("[A-Z][A-Z][A-Z]")) {
                data.add(rowList.get(3));
            }
            else {
                errorFlag = true;
                error = error + "Arrival Code\n";
            }

            int length = (int) (Math.log10(Long.parseLong(rowList.get(4)))+1);
            if(length == 10) {
                Date depTime = new Date(Long.valueOf(rowList.get(4)) * 1000);
                data.add(depTime);
            }
            else {
                errorFlag = true;
                error = error + "Departure Time\n";
            }

            length = (int) (Math.log10(Integer.parseInt(rowList.get(5)))+1);
            if(length >= 1 && length <= 4) {
                data.add(Integer.parseInt(rowList.get(5)));
            }
            else {
                errorFlag = true;
                error = error + "Flight Length\n";
            }

            if(!errorFlag) {
                boolean tmp = true;
                for(Object o : dataEntries) {
                    ArrayList<Object> obj = (ArrayList) o;
                    if(obj.get(1).equals(data.get(1)) && obj.get(0).equals(data.get(0))) {
                        tmp = false;
                    }
                }
                if(tmp) {
                    dataEntries.add(data);
                }
            }
            else {
                System.out.println("[WARNING] Data entry number " + input.indexOf(line) + " has following errors: ");
                System.out.print(error);
                System.out.println("[PREPROCESSOR] Removed erroneous data entry!");
            }
        }
        return dataEntries;
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
