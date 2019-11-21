/**
 * Preprocessor
 * @author Sam Malpass
 */
package courseworkTasks.preprocessFunctions;

import fileHandler.FileHandler;
import mapReduce.Tuple;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

public class Preprocessor {

    private ArrayList<Tuple> airports;
    private ArrayList<String> airportCodes;
    private Levensthein levensthein;

    public Preprocessor() {
        airports = new ArrayList<>();
        airportCodes = new ArrayList<>();
        levensthein = new Levensthein();
        genAirports();
    }

    private boolean checkLatLong(String lat, String lon) {
        int integerPlaces = lat.indexOf(".");
        int decimalPlaces = lat.length() - integerPlaces - 1;
        String tmp = String.format("[0-9]{%d}", decimalPlaces);
        String test = lat.substring(integerPlaces+1, lat.length());
        if(decimalPlaces >= 3 && decimalPlaces <= 13 && test.matches(tmp)) {
            integerPlaces = lon.indexOf(".");
            decimalPlaces = lon.length() - integerPlaces - 1;
            tmp = String.format("[0-9]{%d}", decimalPlaces);
            test = lon.substring(integerPlaces+1, lon.length());
            if(decimalPlaces >=3 && decimalPlaces <= 13 && test.matches(tmp)) {
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

    private String removeBadChars(String x) {
        return x.replaceAll("[^A-Z]","");
    }

    private void genAirports() {
        FileHandler fh = new FileHandler();
        ArrayList<Object> input = fh.read("Data/Top30_airports_LatLong.csv");
        for(Object x : input) {
            String i = (String) x;
            if(!i.isEmpty()) {
                String[] row = i.split(",");
                String pattern = String.format("[A-Z]{%d}", row[0].length());
                if(!(row[0].length() >= 3 && row[0].length() <= 20 && row[0].matches(pattern))){
                    row[0] = removeBadChars(row[0]);
                }
                if(!(row[1].length() == 3 && row[1].matches("[A-Z][A-Z][A-Z]"))){
                    continue;
                }
                if(!checkLatLong(row[2], row[3])) {
                    continue;
                }
                ArrayList<Object> tmp = new ArrayList<>();
                tmp.add(row[0]);
                tmp.add(Double.parseDouble(row[2]));
                tmp.add(Double.parseDouble(row[3]));
                airports.add(new Tuple(row[1], tmp));
                airportCodes.add(row[1]);
            }
        }
    }

    private boolean crossReference(String code) {
        return airportCodes.contains(code);
    }

    public ArrayList<Object> preprocess(Object line) {
        ArrayList<Object> processedLine = new ArrayList<>();
        String castLine = (String) line;
        boolean errorFlag = false;
        String error = "";
        String[] row = castLine.split(",");
        ArrayList<String> rowList = new ArrayList(Arrays.asList(row));

        if(rowList.get(0).matches("[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][A-Z][A-Z][0-9]")) {
            processedLine.add(rowList.get(0));
        }
        else {
            errorFlag = true;
            error = error + "Passenger ID\n";
        }

        if(rowList.get(1).matches("[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][A-Z]")) {
            processedLine.add(rowList.get(1));
        }
        else {
            errorFlag = true;
            error = error + "Flight ID\n";
        }

        if(rowList.get(2).matches("[A-Z][A-Z][A-Z]")) {
            boolean tmp = true;
            if(crossReference(rowList.get(2))) {
                tmp = false;
                processedLine.add(rowList.get(2));
            }
            if(tmp){
                errorFlag = true;
                error = error + "Departure Code\n";
            }
        }
        else {
            Tuple lowestLev = new Tuple(4, null);
            for(String t : airportCodes) {
                int tmp = levensthein.getDistance(t, rowList.get(2));
                if((int) lowestLev.getKey() > tmp) {
                    lowestLev = new Tuple(tmp, t);
                }
            }
            if((int) lowestLev.getKey() <= 1) {
                processedLine.add(lowestLev.getValue());
            }
            else {
                errorFlag = true;
                error = error + "Departure Code\n";
            }
        }

        if(rowList.get(3).matches("[A-Z][A-Z][A-Z]")) {
            boolean tmp = true;
            if(crossReference(rowList.get(3))){
                tmp = false;
                processedLine.add(rowList.get(3));
            }
            if(tmp){
                errorFlag = true;
                error = error + "Arrival Code\n";
            }
        }
        else {
            Tuple lowestLev = new Tuple(4, null);
            for(String t : airportCodes) {
                int tmp = levensthein.getDistance(t, rowList.get(3));
                if((int) lowestLev.getKey() > tmp) {
                    lowestLev = new Tuple(tmp, t);
                }
            }
            if((int) lowestLev.getKey() <= 1) {
                processedLine.add(lowestLev.getValue());
            }
            else {
                errorFlag = true;
                error = error + "Arrival Code\n";
            }
        }

        int length = (int) (Math.log10(Long.parseLong(rowList.get(4)))+1);
        if(length == 10) {
            Date depTime = new Date(Long.valueOf(rowList.get(4)) * 1000);
            processedLine.add(depTime);
        }
        else {
            errorFlag = true;
            error = error + "Departure Time\n";
        }

        length = (int) (Math.log10(Integer.parseInt(rowList.get(5)))+1);
        if(length >= 1 && length <= 4) {
            processedLine.add(Integer.parseInt(rowList.get(5)));
        }
        else {
            errorFlag = true;
            error = error + "Flight Length\n";
        }

        if(!errorFlag) {
            return processedLine;
        }
        else {
            System.out.println("[WARNING] Data entry " + line.toString() + " has following errors\n" + error);
            return null;
        }
    }

    public ArrayList<String> getAirportCodes() {
        return airportCodes;
    }

    public ArrayList<Tuple> getAirports() {
        return airports;
    }
}
