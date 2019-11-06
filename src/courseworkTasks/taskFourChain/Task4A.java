package courseworkTasks.taskFourChain;

import fileHandler.FileHandler;
import mapReduce.Job;
import mapReduce.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class Task4A extends Job {
    private ArrayList<Tuple> allAirports = new ArrayList<>();
    private boolean checkLatLong(String lat, String lon) {
        int integerPlaces = lat.indexOf(".");
        int decimalPlaces = lat.length() - integerPlaces - 1;
        if(decimalPlaces >= 3 && decimalPlaces <= 13) {
             integerPlaces = lon.indexOf(".");
             decimalPlaces = lon.length() - integerPlaces - 1;
             if(decimalPlaces >=3 && decimalPlaces <= 13) {
                 return true;
             }
             else {
                 return false;
             }
        }
        else {
            return false;
        }
    }
    private void getAirports() {
        FileHandler fh = new FileHandler();
        ArrayList<Object> input = fh.read("Data/Top30_airports_LatLong.csv");
        for(Object x : input) {
            String i = (String) x;
            if(!i.isEmpty()) {
                String[] row = i.split(",");
                if(!row[1].isEmpty() && row[1].matches("[A-Z][A-Z][A-Z]")) {
                    if(checkLatLong(row[2],row[3])){
                        ArrayList<Object> loc = new ArrayList<>();
                        loc.add(Double.parseDouble(row[2]));
                        loc.add(Double.parseDouble(row[3]));
                        allAirports.add(new Tuple(row[1], loc));
                    }
                }
            }
        }
    }
    @Override
    public ArrayList<Object> preprocess(ArrayList<Object> input) {
        getAirports();
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
            ArrayList<Object> key = new ArrayList<>();
            key.add(tmp.get(1));
            for(Tuple t : allAirports) {
                ArrayList<Object> tmpCoords;
                if(t.getKey().equals(tmp.get(2))) {
                    tmpCoords = (ArrayList) t.getValue();
                    double startLat = (double) tmpCoords.get(0);
                    double startLong = (double) tmpCoords.get(1);
                    for(Tuple t2 : allAirports) {
                        if(t2.getKey().equals(tmp.get(3))) {
                            tmpCoords = (ArrayList) t2.getValue();
                            double finLat = (double) tmpCoords.get(0);
                            double finLong = (double) tmpCoords.get(1);
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
            System.out.println(t.getKey());
            ArrayList tmp = (ArrayList) t.getKey();
            builder += "Flight " + tmp.get(0) + " travelled " + tmp.get(1) + " nautical miles\n";
        }
        builder += "\n\n";
        return builder;
    }
}
