package courseworkTasks;

import fileHandler.FileHandler;
import mapReduce.Job;
import mapReduce.Tuple;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class Task1 extends Job {
    private ArrayList<String> allAirports = new ArrayList<>();
    private void getAirports() {
        FileHandler fh = new FileHandler();
        ArrayList<String> input = fh.read("Data/Top30_airports_LatLong(1).csv");
        for(String i : input) {
            if(!i.isEmpty()) {
                String[] row = i.split(",");
                if(!row[1].isEmpty() && row[1].matches("[A-Z][A-Z][A-Z]")) {
                    allAirports.add(row[1]);
                }
            }
        }
    }
    private void postprocess(ArrayList<Tuple> arrayList) {
        for(Tuple t : arrayList) {
            allAirports.remove(t.getKey());
        }
    }

    @Override
    public ArrayList<Object> preprocess(ArrayList<String> input) {
        getAirports();
        ArrayList<Object> dataEntries = new ArrayList<>();
        for(String line : input) {
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
                    if(obj.get(1).equals(data.get(1)) && obj.get(2).equals(data.get(2))) {
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
        postprocess(arrayList);
        String builder = "";
        for(Tuple t : arrayList) {
            builder = builder + "Airport ID: " + t.getKey() + "\nNumber of Flights: " + t.getValue() + "\n\n";
        }
        builder = builder + "Missing Airports: \n";
        for(String s : allAirports) {
            builder = builder + s + "\n";
        }
        return builder;
    }


}
