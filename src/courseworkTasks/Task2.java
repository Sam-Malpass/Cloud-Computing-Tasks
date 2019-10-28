package courseworkTasks;

import mapReduce.Job;
import mapReduce.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class Task2 extends Job {
    @Override
    public ArrayList<Object> preprocess(ArrayList<String> arrayList) {
        ArrayList<Object> dataEntries = new ArrayList<>();
        for(String line : arrayList) {
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
                    if(data.equals(obj)) {
                        tmp = false;
                    }
                }
                if(tmp) {
                    dataEntries.add(data);
                }
            }
            else {
                System.out.println("[WARNING] Data entry number " + arrayList.indexOf(line) + " has following errors: ");
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
            ArrayList<Object> tmp = (ArrayList) o;
            ArrayList<Object> vals = new ArrayList<>();
            ArrayList<Object> passengers = new ArrayList<>();
            passengers.add(tmp.get(0));
            ArrayList<Object> rest = new ArrayList<>();
            rest.add(tmp.get(2));
            rest.add(tmp.get(3));
            rest.add(tmp.get(4));
            Date tst = (Date) tmp.get(4);
            long arrivalMill =  tst.getTime() + (((int) tmp.get(5) * 60) * 1000);
            rest.add(new Date(arrivalMill));
            rest.add(tmp.get(5));
            vals.add(passengers);
            vals.add(rest);
            Tuple tuple = new Tuple(tmp.get(1), vals);
            mapperOutput.add(tuple);
        }
        return mapperOutput;
    }

    @Override
    public ArrayList<Tuple> reduce(ArrayList<Tuple> arrayList) {
        ArrayList<Tuple> reducerOutput = new ArrayList<>();
        for(Tuple t : arrayList) {
            ArrayList<Object> vals = (ArrayList) t.getValue();
            ArrayList<Object> passengers = new ArrayList<>();
            ArrayList<Object> details = new ArrayList<>();
            for(Object x : vals) {
                ArrayList<Object> z = (ArrayList) x;
                for (Object o : z) {
                    ArrayList<Object> tmp = (ArrayList) o;
                    if (tmp.size() == 1) {
                        passengers.addAll(tmp);
                    } else {
                        details = tmp;
                    }
                }
            }
            ArrayList<Object> newVals = new ArrayList<>();
            newVals.add(passengers);
            newVals.add(details);
            Tuple tuple = new Tuple(t.getKey(), newVals);
            reducerOutput.add(tuple);
        }
        return reducerOutput;
    }

    @Override
    public String format(ArrayList<Tuple> arrayList) {
        String builder = "";
        for(Tuple t : arrayList) {
            builder += "Flight ID: " + t.getKey() + "\n";
            ArrayList<Object> vals = (ArrayList) t.getValue();
            ArrayList<Object> passengers = (ArrayList) vals.get(0);
            ArrayList<Object> details = (ArrayList) vals.get(1);
            builder += "Departing from " + details.get(0) + " at " + details.get(2) + "\nArriving at " + details.get(1) + " at " + details.get(3) + "\nTotal Time: " + details.get(4) + " minutes\nPassenger List:\n";
            for(Object s : passengers) {
                builder += s + "\n";
            }
            builder += "Total Passengers: " + passengers.size();
            builder+="\n\n";
        }
        return builder;
    }
}
